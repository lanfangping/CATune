
def check(config):

    """
    Constraints:
    shared_buffers > max_wal_size
    effective_cache_size > shared_buffers
    max_connections >= max_prepared_transactions
    superuser_reserved_connections < max_connections
    effective_cache_size ≥ shared_buffers (+ margin)
    wal_buffers <= max_wal_size 
    autovacuum_work_mem <= shared_buffers
    """
    larger = [
        ['shared_buffers', 'max_wal_size'],
        ['effective_cache_size', 'shared_buffers'],
        ['max_connections', 'superuser_reserved_connections'],
    ]

    larger_equal = [
        ['max_connections', 'max_prepared_transactions'],
        ['effective_cache_size', 'shared_buffers'],
        ['max_wal_size', 'wal_buffers'],
        ['shared_buffers', 'autovacuum_work_mem']
    ]

    for left_knob, right_knob in larger:
        if left_knob in config and right_knob in config:
            left_value = config[left_knob]
            right_value = config[right_knob]
            if not left_value > right_value:
                print(f"violates '{left_knob} > {right_knob}'")
        
    for left_knob, right_knob in larger_equal:
        if left_knob in config and right_knob in config:
            left_value = config[left_knob]
            right_value = config[right_knob]
            if not (left_value > right_value or left_value == right_value):
                print(f"violates '{left_knob} >= {right_knob}'")


def is_satisfied(config, rule_list):
    broken_rules = []
    for rule in rule_list:
        satisfied = False
        left_opr, opd, right_opr = rule
        
        if left_opr == 'max_prepared_transactions':
            
            if 'max_prepared_transactions_mode' in config and config['max_prepared_transactions_mode'] == 'disabled':
                left_value = 0
                continue
            
        left_value = config[left_opr]
        right_value = config[right_opr]

        if opd == '<':
            if left_value < right_value:
                satisfied = True
        elif opd == '<=':
            if left_value <= right_value:
                satisfied = True
        elif opd == '>':
            if left_value > right_value:
                satisfied = True
        elif opd == '>=':
            if left_value >= right_value:
                satisfied = True

        if not satisfied:
            broken_rules.append(f"{left_opr} {opd} {right_opr}")
        
    return len(broken_rules) == 0, broken_rules
