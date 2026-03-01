
from abc import ABC, abstractmethod

class LLM(ABC):
    def __init__(self, api_base, api_key, model_type):
        self.api_base = api_base
        self.api_key = api_key
        self.model_type = model_type

    @property
    @abstractmethod
    def current_run_usage(self):
        """ return the current run usage of the LLM model """
        pass
    
    @abstractmethod
    def invoke_api(self):
        """ invoke LLM's API 
        
        Returns:
            response of LLM's API
        """
        pass