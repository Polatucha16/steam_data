from abc import ABC, abstractmethod

class PredictGames(ABC):

    @abstractmethod
    def evaluate(self, masked_sample, game_list):
        pass