import os
import json
import numpy as np
from collections import defaultdict
import matplotlib.pyplot as plt
from scipy.stats import sem

MARKERSIZE=25
line_styles = [
    {'color': 'purple', 'linestyle': '-', 'marker': 'o', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'teal', 'linestyle': '-', 'marker': 'v', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'red', 'linestyle': '-', 'marker': '^', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'navy', 'linestyle': '-', 'marker': 'v', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'black', 'linestyle': '--', 'marker': 's', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'green', 'linestyle': ':', 'marker': 'D', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'blue', 'linestyle': '-.', 'marker': '*', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'brown', 'linestyle': '--', 'marker': 'x', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'orange', 'linestyle': '-', 'marker': 'o', 'markersize':MARKERSIZE, 'linewidth':3},
    {'color': 'pink', 'linestyle': '-', 'marker': '^', 'markersize':MARKERSIZE, 'linewidth':3},
]

def load_and_process(data, type, historical_best=True):
    performance = []
    for d in data:
        # print(d[4])
        try:
            cost = d['cost']
        except:
            cost = d[4]
        if type == 'throughput':
            if isinstance(cost, list) : # multi-objective
                if cost[0] >= 0: 
                    performance.append(0)
                else:
                    performance.append(-cost[0])
            else:
                if cost >= 0:
                    performance.append(0)
                else:
                    performance.append(-cost)
        elif type == 'latency':
            if isinstance(cost, list) : # multi-objective
                performance.append(cost[0] / 1000000)
            else:
                performance.append(cost/ 1000000)
        else:
            performance.append(0)
    
    if historical_best:
        # print("performance", performance)
        if type == 'throughput':
            historical_best_performance = [max(performance[:i+1]) for i in range(len(performance))]
        else:
            historical_best_performance = [min(performance[:i+1]) for i in range(len(performance))]
        return historical_best_performance
    else:
        return performance

def load_and_get_historical_best_data(folder_name, seed=100, type='throughput', tag=None):
    # print(folder_name)
    fine_path = f'./{folder_name}/postgres/fine{"_"+tag if tag else ""}/{seed}/runhistory.json' # tpcc sf20, t10
    coarse_path = f'./{folder_name}/postgres/coarse{"_"+tag if tag else ""}/{seed}/runhistory.json'
    performance = []
    historical_best_performance = []
    with open(coarse_path, 'r') as f:
        file_data = json.load(f)
        data = file_data['data']
        performance.extend(load_and_process(data, type))
    with open(fine_path, 'r') as f:
        file_data = json.load(f)
        data = file_data['data']
        performance.extend(load_and_process(data[30:], type))
    
    # print("performance", performance)
    if type == 'throughput':
        historical_best_performance = [max(performance[:i+1]) for i in range(len(performance))]
    else:
        historical_best_performance = [min(performance[:i+1]) for i in range(len(performance))]

    return historical_best_performance, performance

def load_and_get_data_with_deviation_from_project_data(project_name, workload):
    project_data = json.load(open(f'experiments_results/{workload}/project_data.json', 'r'))
    data = []
    for performance_tuple in project_data[project_name]['performance_tuple']:
        data.append(performance_tuple[0])
    
    # Convert to numpy array for easier computation
    arr = np.array(data)

    # Calculate mean and standard deviation per index (column-wise)
    means = np.mean(arr, axis=0)
    stds = np.std(arr, axis=0)

    return means, stds

def get_mean_sem(performance_lists):
    # Convert to numpy array for easier computation
    arr = np.array(performance_lists)

    # Compute SEM along axis=0 (per index / per column)
    means = np.mean(arr, axis=0)
    sem_per_index = sem(arr, axis=0)
    return means, sem_per_index


def show(data_lists:list, labels:list, type:str='throughput'):
    # Re-plot with x-axis label changed to "Iteration"
    plt.figure(figsize=(12, 6))

    for i in range(len(data_lists)):
        historical_best_performance = data_lists[i]
        label = labels[i]
        style = line_styles[i%len(line_styles)]
        
        # Plot the line for past best throughputs
        plt.plot(range(len(historical_best_performance)), historical_best_performance, label=label, markevery=10, **style)

    # Updated x-axis label
    plt.xlabel('Iteration')
    plt.ylabel(type)
    plt.legend()
    plt.grid(True)
    # plt.savefig(output_file)
    plt.show()

def show_with_std(data_lists:list, labels:list, *,
                  marker_size=10, 
                  type:str='throughput', 
                  font_size=55, 
                  legend=True, 
                  bbox_to_anchor=(0.5, 1.32),
                  ylabel_anchor=(-0.12, 0.4),
                  firstpoint=False
                  ):
    
    # Re-plot with x-axis label changed to "Iteration"
    plt.figure(figsize=(12, 8))

    point = 0
    if firstpoint:
        points = []
        for mean, std in data_lists:
            points.append(mean[0])
        if type == 'throughput':
            point = min(points)
        else:
            point = max(points)
    for i in range(len(data_lists)):
        mean, std = data_lists[i]
        if firstpoint:
            mean[0] = point
        label = labels[i]
        style = line_styles[i%len(line_styles)]
        style['markersize'] = marker_size
        
        # Plot the line for past best throughputs
        plt.plot(range(len(mean)), mean, label=label, markevery=15, **style)
        # Plot the std deviation as a shaded area
        plt.fill_between(range(len(mean)), mean - std, mean + std, color=style['color'], alpha=0.1)
        # plt.errorbar(range(len(mean)), mean, yerr=std, fmt='-', capsize=3)
        # plt.plot(range(len(mean)), mean + std, linestyle='-', color=style['color'], alpha=0.1)
        # plt.plot(range(len(mean)), mean - std, linestyle='-', color=style['color'], alpha=0.1)

    # Updated x-axis label
    plt.xlabel('Iteration', fontsize=font_size)
    if type == 'throughput':
        plt.ylabel('Throughput (tps)', fontsize=font_size)
    elif type == 'latency':
        plt.ylabel('Avg. Latency (s)', fontsize=font_size)
    plt.gca().yaxis.set_label_coords(ylabel_anchor[0], ylabel_anchor[1])
    plt.xticks(np.arange(0, len(data_lists[i][0])+1, 50), fontsize=font_size-5)
    plt.yticks(fontsize=font_size-5)
    if legend:
        plt.legend(fontsize=font_size-10, 
                #    loc='best', 
                   loc='upper center',
                   ncols=len(labels),
                bbox_to_anchor=bbox_to_anchor,
                )
    plt.grid(True)
    # plt.savefig(output_file)
    plt.show()

def maintain_data(folder="experiments_results/tpcc", type='throughput'):
    project_data = defaultdict(lambda: defaultdict(list))
    for folder_name in os.listdir(folder):
        if folder_name == 'figures' or folder_name == 'project_data.json':
            continue
        # print(folder_name)
        items = folder_name.split('_')
        if items[-1].isdigit():
            project_name = '_'.join(items[:-1])
            date_id = items[-1]
        else:
            project_name = folder_name
            date_id = ''

        data_tuple = None
        history_file = os.path.join(folder, folder_name)
        # print(history_file)
        try:
            data_tuple = load_and_get_historical_best_data(history_file, type=type)
        except FileNotFoundError as e:
            print(e)
            continue
        project_data[project_name]['performance_tuple'].append(data_tuple)
        project_data[project_name]['date_ids'].append(date_id)

    json.dump(project_data, open(os.path.join(folder, 'project_data.json'), 'w'))        


if __name__ == '__main__':
    
    # #==================overall================
    # num_plots = 7
    # files = [
    #     ("experiments_results/tpcc/deepseek-v3-overall_202504101721", 100),
    #     ("experiments_results/tpcc/deepseek-v3-overall_202504131541", 100),
    #     ("experiments_results/tpcc/gpt-4o-overall_202504102122", 100),
    #     ("experiments_results/tpcc/gpt-4o-overall_202504131536", 100), 
    #     ("experiments_results/tpcc/gpt4-4o-mini-overall_202504101933", 100),
    #     ("experiments_results/tpcc/gpt-4-previous-good-knowledge", 100),
    #     ("experiments_results/tpcc/gpt-3.5-turbo-overall_202504140003", 100),
    # ]
    # labels = [
    #     "Deepseek-v3-1", 
    #     "Deepseek-v3-2", 
    #     "GPT-4o-1", 
    #     "GPT-4o-2", 
    #     "GPT-4o-mini",
    #     "Previous Good(GPT-4)",
    #     "GPT-3.5-turbo"
    # ]
    # output_type = 'throughput'
    # output_file = "experiments_results/tpcc/figures/end_to_end_study/overall_compare_7.png"
    # data_lists = []
    # for i in range(num_plots):
    #     folder_name, seed = files[i]
    #     data = load_and_get_historical_best_data(folder_name=folder_name, seed=seed, type=output_type)
    #     data_lists.append(data)
    
    # show(data_lists=data_lists, labels=labels, output_file=output_file, num_plots=num_plots, type=output_type)

    #==================overall================
    # num_plots = 5
    # files = [
    #     ("experiments_results/tpcc/gpt-4-previous-good-knowledge", 100),
    #     ("experiments_results/tpcc/ks-gpt4-kr--sv-gpt3.5turbo-st-gpt4-spv-gpt4_202505081057", 100),
    #     ("experiments_results/tpcc/ks-gpt4-kr--sv-gpt4-st-gpt3.5turbo-spv-gpt4_202505081616", 100),
    #     ("experiments_results/tpcc/ks-gpt4-kr--sv-gpt4-st-gpt4-spv-gpt3.5turbo_202505091059", 100), 
    #     ("experiments_results/tpcc/ks-gpt4-kr-gpt3.5turbo_202505011227", 100)
    # ]
    # labels = [
    #     "KS-GPT4, SV-GPT4, SR-GPT4, SPV-GPT4", 
    #     "KS-GPT4, SV-GPT3.5-turbo, SR-GPT4, SPV-GPT4",
    #     "KS-GPT4, SV-GPT4, SR-GPT3.5-turbo, SPV-GPT4",
    #     "KS-GPT4, SV-GPT4, SR-GPT4, SPV-GPT3.5-turbo",
    #     "KS-GPT4, SV-GPT3.5-turbo, SR-GPT3.5-turbo, SPV-GPT3.5-turbo"
    # ]

    project_names = [
        "deepseek-v3-overall",
        "gpt-3.5-turbo-overall",
        "gpt4-4o-mini-overall",
        "gpt-4o-overall",
        "claude-sonnet4-overall",
        "gpt-4-previous"
    ]
    labels = [
        "DeepSeekV3", 
        "GPT3.5-turbo", 
        "GPT4o-mini",
        "GPT4o",
        "Claude Sonnet4",
        "GPT4"
    ]
    num_plots = len(project_names)
    output_type = 'throughput'
    workload='tpcc'
    maintain_data(folder=f"experiments_results/{workload}", type=output_type)
    output_file = f"experiments_results/{workload}/figures/end_to_end_study/overall_study_std_{num_plots}.png"
    data_lists = []
    for i in range(num_plots):
        project_name = project_names[i]
        data = load_and_get_data_with_deviation_from_project_data(project_name=project_name, workload=workload)
        data_lists.append(data)
    
    show_with_std(data_lists=data_lists, labels=labels, output_file=output_file, num_plots=num_plots, type=output_type)

    

