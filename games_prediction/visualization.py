from abc import ABC, abstractmethod
import numpy as np
import matplotlib.pyplot as plt

class VisualizationStrategy(ABC):
    @abstractmethod
    def plot(self, result:np.ndarray, true_values:np.ndarray, game_names:list, **kwargs):
        pass

class VerticalBarPlot(VisualizationStrategy):

    def plot(self, predicted, true_values, game_names):
        true_order = np.argsort(true_values)[::-1]
        true_values = true_values[true_order]
        predicted = predicted[true_order]
        name_list = [game_names[int(i)] for i in true_order]
        ranks_to_plot = {
            "True" : true_values,
            "Predicted" : predicted
        }
        x = np.arange(len(name_list))  # the label locations
        width = 0.5  # the width of the bars
        multiplier = 0.5

        fig, ax = plt.subplots(figsize=(25, 10), layout='constrained')
        for attribute, measurement in ranks_to_plot.items():
            offset = width * multiplier
            rects = ax.bar(x + offset, measurement, width, label=attribute)
            # ax.bar_label(rects, padding=3) fmt
            ax.bar_label(rects, padding=3, fmt= lambda x: f"{x:.0f}",
                        color='tab:blue' if attribute == "True" else 'tab:orange',
                        size='xx-large')
            multiplier += 1
        ax.set_ylabel('rank')
        ax.set_title('predicted and true time spent')
        ax.set_xticks(x + width, name_list)
        ax.set_xticklabels(name_list, rotation = 45, ha="right")
        ax.legend(loc='upper left', ncols=2, prop={'size': 25})# fontsize='xx-large'
        ax.set_ylim(0, 5)

        plt.show()