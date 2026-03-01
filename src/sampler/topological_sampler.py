"""
Unify topology-aware sampling into a single TopoSampler, then reuse it in:
1) SMAC3 initial design (TopoLatinHypercubeInitialDesign) by calling sampler.sample_lhs(n)
2) ConfigSpace sampling by calling sampler.sample(size)

This avoids duplicated logic and guarantees consistent behavior.

Assumptions / Scope
-------------------
- Constraints are pairwise inequalities: (a, op, b) with op in {"<","<=",">",">="}
- Mapping:
    (a, "<",  b)  => b -> a
    (a, "<=", b)  => b -> a
    (a, ">",  b)  => a -> b
    (a, ">=", b)  => a -> b
- Conditional activation is provided explicitly as:
    (child, parent, {"enabled", ...})
  and is enforced by skipping inactive children (not assigning values).
- Handles numeric hps (UniformInteger/Float) + categorical gates.
- Final validation is done via ConfigSpace Configuration(...) to catch other forbiddens.

NOTE: Monkey-patching ConfigSpace.sample_configuration is supported via a wrapper.
Do NOT try to subclass the C-accelerated internals; just wrap/patch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Set, Optional, Any
from collections import defaultdict, deque
import math
import numpy as np

from ConfigSpace.configuration_space import Configuration, ConfigurationSpace
from ConfigSpace.util import ForbiddenValueError
from scipy.stats.qmc import LatinHypercube

# SMAC3
from smac.initial_design.abstract_initial_design import AbstractInitialDesign


# ----------------------------
# Types
# ----------------------------
Constraint = Tuple[str, str, str]
ConditionalActivation = Tuple[str, str, Set[str]]  # (child, parent, active_values)


@dataclass(frozen=True)
class EdgeConstraint:
    parent: str
    child: str
    op: str  # "<", "<=", ">", ">="


# ----------------------------
# Graph utils
# ----------------------------
def build_topology_graph(constraints: List[Constraint]):
    children_of: Dict[str, List[EdgeConstraint]] = defaultdict(list)
    indegree: Dict[str, int] = defaultdict(int)
    nodes: Set[str] = set()

    for left, op, right in constraints:
        if op not in ("<", "<=", ">", ">="):
            raise ValueError(f"Unsupported op: {op}")
        nodes.add(left)
        nodes.add(right)

        # normalize to parent -> child
        if op in ("<", "<="):   # a < b => b -> a
            parent, child = right, left
            edge_op = op
        else:                   # a > b => a -> b
            parent, child = left, right
            edge_op = op

        children_of[parent].append(EdgeConstraint(parent=parent, child=child, op=edge_op))
        indegree[child] += 1
        indegree.setdefault(parent, 0)

    return children_of, indegree, nodes


def topo_sort(children_of: Dict[str, List[EdgeConstraint]], indegree: Dict[str, int]) -> List[str]:
    q = deque([n for n, d in indegree.items() if d == 0])
    order: List[str] = []
    while q:
        u = q.popleft()
        order.append(u)
        for e in children_of.get(u, []):
            v = e.child
            indegree[v] -= 1
            if indegree[v] == 0:
                q.append(v)

    if len(order) != len(indegree):
        remaining = [n for n, d in indegree.items() if d > 0]
        raise ValueError(f"Cycle detected among nodes: {remaining}")
    return order




# ----------------------------
# TopoSampler (single source of truth)
# ----------------------------
class TopoSampler:
    """
    Shared sampler used by both SMAC initial design and ConfigSpace sampling.

    Key methods:
      - sample_one(): one Configuration
      - sample(size): list[Configuration] (or single if size is None)
      - sample_lhs(n): returns list[Configuration] with LHS on roots, topo on others
    """

    def __init__(
        self,
        cs: ConfigurationSpace,
        constraints: List[Constraint],
        conditional_activations: Optional[List[ConditionalActivation]] = None,
        *,
        seed: Optional[int] = None,
        max_attempts_per_config: int = 5000,
        strategy: str = 'adptive',
        # You can tune these: larger alpha => more near-parent / near-upper
        child_beta_alpha: float = 5.0,
        child_beta_beta: float = 1.0
    ):
        self.cs = cs
        self.constraints = constraints
        self.children_of, indeg, nodes = build_topology_graph(constraints)
        self.topo = topo_sort(self.children_of, dict(indeg))
        self.nodes = nodes
        self.strategy = strategy

        # child -> (parent, active_values)
        self.cond: Dict[str, tuple[str, Set[str]]] = {}
        for child, parent, active_vals in (conditional_activations or []):
            self.cond[child] = (parent, set(active_vals))
        # restrict to nodes in cs
        self.topo = [n for n in self.topo if n in cs]
        self.nodes = {n for n in self.nodes if n in cs}

        # compute roots in cs
        indeg2 = {n: 0 for n in self.nodes}
        for p, edges in self.children_of.items():
            if p not in indeg2:
                continue
            for e in edges:
                if e.child in indeg2:
                    indeg2[e.child] += 1
        self.roots = [n for n, d in indeg2.items() if d == 0]

        # extend roots with independent knobs
        independent_knobs = [hp.name for hp in cs.get_hyperparameters() if hp.name not in self.nodes]
        self.roots.extend(independent_knobs) 

        # count nodes in the longest path per node
        self.count_longest_path: Dict[str, int] = {}
        for n in reversed(self.topo):
            max_child_path = 0
            for e in self.children_of.get(n, []):
                child = e.child
                child_path = self.count_longest_path.get(child, 0)
                if child_path + 1 > max_child_path:
                    max_child_path = child_path + 1
            self.count_longest_path[n] = max_child_path

        # RNG
        if seed is not None:
            self.rng = np.random.RandomState(seed)
        else:
            self.rng = getattr(cs, "random", np.random.RandomState())

        self.max_attempts_per_config = max_attempts_per_config
        self.child_beta_alpha = child_beta_alpha
        self.child_beta_beta = child_beta_beta

    def _is_active(self, name: str, assigned: Dict[str, Any]) -> bool:
        if name not in self.cond:
            return True
        parent, active_vals = self.cond[name]
        if parent not in assigned:
            return False
        return str(assigned[parent]) in active_vals

    def _sample_hp_unconstrained(self, name: str) -> Any:
        hp = self.cs[name]
        if hasattr(hp, "choices"):
            return self.rng.choice(list(hp.choices))
        if self._has_bounds(hp):
            lo, hi = self._base_bounds(self.cs, name)
            return self._sample_uniform(self.rng, lo, hi, is_int=self._is_int_hp(hp))
        # constants / others
        return getattr(hp, "default_value", None)

    def _build_values_topo(self, root_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        cs = self.cs
        values: Dict[str, Any] = {}

        # sample roots first (unless overridden)
        for r in self.roots:
            if root_overrides and r in root_overrides:
                values[r] = root_overrides[r]
            else:
                values[r] = self._sample_hp_unconstrained(r)

        for r in self.roots:
            if not self._is_active(r, values):
                values.pop(r, None)
                # print(f"Root {r} is inactive, skipping assignment.")
                continue

        # sample remaining nodes in topo order
        for name in self.topo:
            if name in values:
                continue
            if not self._is_active(name, values):
                continue
            hp = cs[name]

            # categorical
            if hasattr(hp, "choices"):
                values[name] = self.rng.choice(list(hp.choices))
                continue

            # numeric
            if self._has_bounds(hp):
                lo, hi = self._base_bounds(cs, name)

                # apply constraints from assigned parents
                for parent, edges in self.children_of.items():
                    if parent not in values:
                        continue
                    if not self._is_active(parent, values):
                        continue
                    pval = values[parent]
                    for e in edges:
                        if e.child != name:
                            continue
                        lo, hi = self._apply_parent_to_child_interval(cs, name, lo, hi, pval, e.op)

                # values[name] = _sample_uniform(self.rng, lo, hi, is_int=_is_int_hp(hp))
                # values[name] = _sample_skewed_toward_upper(self.rng, lo, hi, is_int=_is_int_hp(hp),  alpha=self.child_beta_alpha, beta=self.child_beta_beta,)
                if self.strategy == 'adaptive':
                    print("Topo sampling - Adaptive")
                    hp_value = self._sample_adaptive_skewed(self.rng, lo, hi, is_int=self._is_int_hp(hp), nodes_longest_path=self.count_longest_path.get(name,0))
                elif self.strategy == 'uniform':
                    print("Topo sampling - Uniform")
                    hp_value = values[name] = self._sample_uniform(self.rng, lo, hi, is_int=self._is_int_hp(hp))
                else:
                    print(f"Unknown strategy: '{self.strategy}'")
                    exit()
                values[name] = hp_value
                continue

            values[name] = getattr(hp, "default_value", None)

        return values

    def sample_one(self) -> Configuration:
        # if cs has other forbiddens not represented by our constraints, we may need retries
        for _ in range(self.max_attempts_per_config):
            try:
                values = self._build_values_topo()
                return Configuration(self.cs, values=values)
            except ForbiddenValueError as e:
                print(f"ForbiddenValueError: {e}. Retrying...")
                continue
            except Exception as e:
                print(f"Sampling exception: {e}. Retrying...")
                continue
        raise RuntimeError(
            f"TopoSampler: failed to sample a valid config in {self.max_attempts_per_config} attempts. "
            "Likely remaining forbiddens/conditions not captured, or infeasible constraints."
        )

    def sample(self, size: Optional[int] = None):
        """
        regular constrained random sampling
        Samples each root independently and uniformly
        Then samples children topo-aware, tightening bounds based on parents
        Behaves like a replacement for ConfigSpace.sample_configuration()
        """
        print(f"{self.__class__.__name__}: sampling {size if size is not None else 1} configurations...")
        if size is None:
            return self.sample_one()
        if not isinstance(size, int):
            raise TypeError(f"Expected int or None, got {type(size)}")
        if size < 1:
            return []
        return [self.sample_one() for _ in range(size)]

    # ---- LHS on roots ----
    # def _lhs_unit(self, n: int, d: int) -> np.ndarray:
    #     X = np.zeros((n, d), dtype=float)
    #     for j in range(d):
    #         perm = self.rng.permutation(n)
    #         X[:, j] = (perm + self.rng.rand(n)) / n
    #     return X
    
    def _lhs_unit(self, d: int, n: int) -> np.ndarray:
        """
        Generate an n x d Latin Hypercube design in [0, 1)^d using SciPy's QMC LatinHypercube,
        consistent with LatinHypercubeInitialDesign._select_configurations.
        """
        # Keep seed behavior aligned with _select_configurations (draw an int from self._rng)
        seed = int(self.rng.randint(0, 1000000))
        lhd = LatinHypercube(d=d, seed=seed)
        return lhd.random(n=n)

    def _map_unit_to_hp(self, name: str, u: float) -> Any:
        hp = self.cs[name]
        if self._has_bounds(hp):
            lo, hi = float(hp.lower), float(hp.upper)
            if self._is_int_hp(hp):
                v = int(lo + math.floor(u * (hi - lo + 1)))
                return int(min(max(v, int(lo)), int(hi)))
            if self._is_float_hp(hp):
                return float(lo + u * (hi - lo))
            return float(lo + u * (hi - lo))
        if hasattr(hp, "choices"):
            choices = list(hp.choices)
            idx = int(min(len(choices) - 1, math.floor(u * len(choices))))
            return choices[idx]
        return getattr(hp, "default_value", None)

    def sample_lhs(self, n: int) -> List[Configuration]:
        """
        Generate n configs, using LHS only for roots (active roots only).
        Children are sampled topo-aware with constraint tightening.
        """
        roots = [r for r in self.roots if r in self.cs]
        if n <= 0:
            return []

        U = self._lhs_unit(n=n, d=len(roots)) if roots else np.zeros((n, 0))
        configs: List[Configuration] = []

        max_attempts = max(self.max_attempts_per_config, 50 * n)
        attempts = 0
        i = 0
        while len(configs) < n and attempts < max_attempts:
            attempts += 1

            # root overrides from LHS row i
            overrides: Dict[str, Any] = {}
            row = U[i % n] if roots else []
            for j, r in enumerate(roots):
                overrides[r] = self._map_unit_to_hp(r, float(row[j]))

            try:
                values = self._build_values_topo(root_overrides=overrides)
                cfg = Configuration(self.cs, values=values)
                cfg.origin = "Initial Design: TopoSampler(LHS-roots)"
                configs.append(cfg)
            except ForbiddenValueError as e:
                print(f"ForbiddenValueError: {e}")
                i += 1
                continue
            except Exception as e:
                print(e)
                i += 1
                continue

            i += 1

        return configs

    # ----------------------------
    # Hyperparameter helpers
    # ----------------------------
    def _has_bounds(self, hp) -> bool:
        return hasattr(hp, "lower") and hasattr(hp, "upper")

    def _is_int_hp(self, hp) -> bool:
        return self._has_bounds(hp) and isinstance(hp.lower, int) and isinstance(hp.upper, int)

    def _is_float_hp(self, hp) -> bool:
        return self._has_bounds(hp) and isinstance(hp.lower, float) and isinstance(hp.upper, float)

    def _base_bounds(self, cs: ConfigurationSpace, name: str) -> tuple[float, float]:
        hp = cs[name]
        if not self._has_bounds(hp):
            raise ValueError(f"HP {name} has no numeric bounds.")
        return float(hp.lower), float(hp.upper)

    def _intersect(self, lo: float, hi: float, lo2: float, hi2: float) -> tuple[float, float]:
        return max(lo, lo2), min(hi, hi2)

    def _sample_uniform(self, rng: np.random.RandomState, lo: float, hi: float, is_int: bool):
        if lo > hi:
            raise ValueError(f"Empty interval [{lo}, {hi}]")
        if is_int:
            lo_i = int(math.ceil(lo))
            hi_i = int(math.floor(hi))
            if lo_i > hi_i:
                raise ValueError(f"Empty int interval [{lo_i}, {hi_i}] from [{lo}, {hi}]")
            return int(rng.randint(lo_i, hi_i + 1))
        return float(rng.uniform(lo, hi))

    def _sample_skewed_toward_upper(self,
            rng,
            lo: float,
            hi: float,
            is_int: bool,
            *,
            alpha: float = 5.0,
            beta: float = 1.0,
        ):
            """
            Sample from [lo, hi] but skew probability mass toward hi using Beta(alpha, beta).
            alpha > beta => skew to 1 (upper end). alpha=beta=1 => uniform.

            For ints: discretize after mapping.
            """
            if lo > hi:
                raise ValueError(f"Empty interval [{lo}, {hi}]")
            if lo == hi:
                return int(lo) if is_int else float(lo)

            u = float(rng.beta(alpha, beta))  # in (0,1)
            x = lo + u * (hi - lo)

            if is_int:
                # Convert to a valid integer in [ceil(lo), floor(hi)]
                lo_i = int(math.ceil(lo))
                hi_i = int(math.floor(hi))
                if lo_i > hi_i:
                    raise ValueError(f"Empty int interval [{lo_i}, {hi_i}] from [{lo}, {hi}]")
                xi = int(round(x))
                if xi < lo_i: xi = lo_i
                if xi > hi_i: xi = hi_i
                return xi

            return float(x)

    def _sample_adaptive_skewed(self,
            rng,
            lo: float,
            hi: float,
            is_int: bool,
            *,
            nodes_longest_path: int,
            base_alpha = 1.0,
            base_beta = 1.0,
        ):
            """
            Sample from [lo, hi] but skew probability mass toward hi using Beta(alpha, beta).
            alpha > beta => skew to 1 (upper end). alpha=beta=1 => uniform.

            Depth increases skewness to upper end.

            For ints: discretize after mapping.
            """
            if lo > hi:
                raise ValueError(f"Empty interval [{lo}, {hi}]")
            if lo == hi:
                return int(lo) if is_int else float(lo)

            # Increase alpha with depth
            alpha = base_alpha + nodes_longest_path
            beta = base_beta

            return self._sample_skewed_toward_upper(rng, lo, hi, is_int, alpha=alpha, beta=beta)

    def _apply_parent_to_child_interval(self,
        cs: ConfigurationSpace,
        child: str,
        lo: float,
        hi: float,
        parent_val: Any,
        op: str,
    ) -> tuple[float, float]:
        """
        For a parent -> child edge:
        - If original tuple was child < parent (stored "<" or "<="), enforce child <=/< parent_val.
        - If original tuple was parent > child (stored ">" or ">="), that's equivalent to child <(=) parent too.
        Therefore all ops imply an *upper bound* on child from parent_val.
        """
        hp_child = cs[child]
        is_int = self._is_int_hp(hp_child)
        eps = 1.0 if is_int else 1e-12

        if op in ("<", ">"):        # strict => child < parent
            return self._intersect(lo, hi, -math.inf, float(parent_val) - eps)
        if op in ("<=", ">="):      # non-strict => child <= parent
            return self._intersect(lo, hi, -math.inf, float(parent_val))
        raise ValueError(op)





# ----------------------------
# Example wiring
# ----------------------------
if __name__ == "__main__":
    # cs = ... (your ConfigurationSpace)

    constraints = [
        ("max_wal_senders", "<", "max_connections"),
        ("superuser_reserved_connections", "<", "max_connections"),
        ("shared_buffers", "<=", "max_wal_size"),
        ("max_prepared_transactions", ">=", "max_connections"),
        ("min_wal_size", "<=", "max_wal_size"),
        ("max_parallel_workers_per_gather", "<=", "max_parallel_workers"),
        ("max_parallel_workers_per_gather", "<", "max_worker_processes"),
        ("max_parallel_maintenance_workers", "<", "max_parallel_workers"),
        ("max_parallel_workers", "<=", "max_worker_processes"),
        ("max_logical_replication_workers", "<", "max_worker_processes"),
        ("max_sync_workers_per_subscription", "<=", "max_logical_replication_workers"),
        ("vacuum_freeze_table_age", "<", "autovacuum_freeze_max_age"),
        ("vacuum_freeze_min_age", "<", "autovacuum_freeze_max_age"),
        ("vacuum_multixact_freeze_table_age", "<", "autovacuum_multixact_freeze_max_age"),
        ("vacuum_multixact_freeze_min_age", "<", "autovacuum_multixact_freeze_max_age"),
    ]

    conditional_activations = [
        ("max_prepared_transactions", "max_prepared_transactions_mode", {"enabled"}),
    ]

    # sampler = TopoSampler(cs, constraints, conditional_activations, seed=0)

    # Patch ConfigSpace sampling:
    # attach_topo_sampler_to_configspace(cs, sampler)

    # In SMAC:
    # scenario = Scenario(configspace=cs, n_trials=200, seed=0)
    # init_design = TopoLatinHypercubeInitialDesign(scenario=scenario, topo_sampler=sampler, n_configs=10, seed=100)
    # smac = HPOFacade(scenario=scenario, target_function=target_fn, initial_design=init_design)
    # smac.optimize()

    pass