from abc import ABC, abstractmethod

class PredictionScheme(ABC):

    @abstractmethod
    def users(self):
        pass

    @abstractmethod
    def game_names(self):
        pass

    @abstractmethod
    def mask(self):
        pass