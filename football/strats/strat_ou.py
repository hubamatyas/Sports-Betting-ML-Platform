import json
import numpy as np

class StratOU:
    def __init__(self, ladders):
        self.ladders = ladders
        self.begin_first_half = None
        self.end_first_half = None
        self.begin_second_half = None
        self.current_timestamp = np.inf
        self.milliseconds = 0.0


    def track_score(self):
        pass

    def track_time(self, pt, ladder):

        marketDefinition = ladder.get('marketDefinition')
        if marketDefinition:
            if marketDefinition['in_play'] and self.begin_first_half is None:
                # set begin_in_play to key
                self.begin_first_half = pt
                self.current_timestamp = pt
                print(f"begin_in_play: {self.begin_first_half}")  
            
            if not marketDefinition['in_play'] and self.begin_first_half and marketDefinition['turn_inplay_enabled'] and self.end_first_half is None:
                # set end_in_play to key
                self.end_first_half = pt
                print(f"end_in_play: {self.end_first_half}")
            
        if pt > self.current_timestamp:

            self.milliseconds += pt - self.current_timestamp
            self.current_timestamp = pt
        
        
    def run(self):
    
        for ladder_dict in self.ladders:
            for pt, ladder in ladder_dict.items():
                self.track_time(int(pt), ladder)
        
        print(self.milliseconds/1000/60)
    

if __name__ == "__main__":
    with open('sample_ladderdata.json', 'r') as f:
        ladders = json.load(f)

    strat = StratOU(ladders['ladders'])
    strat.run()
