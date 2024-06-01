from games_prediction.model import PredictGames

class ModelEvaluator:
    connection = 0

    def __init__(self, model:PredictGames, scheme):
        self.model = model
        self.scheme = scheme
    
    def evaluate(self):
        whom = self.scheme.users()
        features, labels = self.scheme.game_names()