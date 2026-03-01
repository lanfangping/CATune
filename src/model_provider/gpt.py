from openai import OpenAI, APIError
import os
import sys
import time
import random
from model_provider.llm import LLM
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

class KnobImportance(BaseModel):
    knob_name: str
    importance: float
    reason: str

class KnobImportanceList(BaseModel):
    knob_importance_list: list[KnobImportance]

class KnobRange(BaseModel):
    name: str
    type: str
    min_value: float
    max_value: float
    values: list[str]
    reason: str

class KnobRangeList(BaseModel):
    knob_range_list: list[KnobRange]

class GPT(LLM):
    def __init__(self, api_base, api_key, model_type="gpt-4o-mini"):
        super().__init__(api_base=api_base, api_key=api_key, model_type=model_type)
        model_id_mapping = {
            "gpt-5-nano": "gpt-5-nano",
            "gpt-5-mini": "gpt-5-mini",
            "gpt-5": "gpt-5",
            "gpt-4o-mini": "gpt-4o-mini",
            "gpt-4o": "gpt-4o",
            "gpt-4.1": "gpt-4.1",
            "gpt-3.5-turbo": "gpt-3.5-turbo-1106"
        }
        self.model = model_id_mapping.get(model_type, "gpt-4o-mini")
        self.money = 0
        self.token = 0
        self.input_token = 0
        self.output_token = 0
        self.cur_token = 0
        self.cur_money = 0

        self._current_run_usage = None

    def invoke_api(self, system_prompt, prompt, output_format, n=3, log=None):
        if n <= 0:
            print("Call API failure.")
            exit()
        
        client = OpenAI(api_key=self.api_key, base_url = self.api_base)
        try:
            response = client.responses.parse(
                model=self.model,
                input=[
                    {
                        "role": "system",
                        "content": f"{system_prompt}",
                    },
                    {"role": "user", "content": f"{prompt}"},
                ],
                text_format=output_format,
                # temperature=0.2 # make results be deterministic
            )
        except APIError as e:
            print("Call API fail:", e)
            exit()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(f"Exception Type: {exc_type.__name__}")
            print(f"Exception Message: {str(e)}")
            print(f"Occurred at Line: {exc_tb.tb_lineno}")
            print("Sleeping...")
            time.sleep(random.randint(30, 40))
            print("retry.")
            return self.invoke_api(system_prompt, prompt, output_format, n=n-1)
        self._update_current_run_usage(response)
        return response.output_parsed.dict()

    def _update_current_run_usage(self, response):
        
        if self._current_run_usage is None:
            self._current_run_usage = {
                "input_token": response.usage.input_tokens,
                "output_token": response.usage.output_tokens,
                "total": response.usage.total_tokens
            }
        else:
            self._current_run_usage.update({
                "input_token": response.usage.input_tokens,
                "output_token": response.usage.output_tokens,
                "total": response.usage.total_tokens
            })

    @property
    def current_run_usage(self):
        if self._current_run_usage is None:
            return {
                "input_token": 0,
                "output_token": 0,
                "total": 0
            }
        return self._current_run_usage

    # def calc_token(self, in_text, out_text=""):
    #     if isinstance(in_text, dict):
    #         in_text = json.dumps(in_text)
        
    #     if isinstance(out_text, dict):
    #         out_text = json.dumps(out_text)
            
    #     if self.model == 'deepseek-chat':
    #         chat_tokenizer_dir = "./src/knowledge_handler/deepseek_v3_tokenizer"
    #         enc =  transformers.AutoTokenizer.from_pretrained( 
    #                 chat_tokenizer_dir, trust_remote_code=True
    #                 )
    #     elif self.model.startswith('claude'):
    #         client = anthropic.Anthropic()
            
    #         # Use the count_tokens method
    #         in_token_count = client.messages.count_tokens(
    #             model=self.model,
    #             messages=[
    #                 {"role": "user", "content": in_text}
    #             ]
    #         )
    #         input_token = in_token_count.input_tokens
    #         self.input_token += input_token

    #         out_token_count = client.messages.count_tokens(
    #             model=self.model,
    #             messages=[
    #                 {"role": "user", "content": in_text}
    #             ]
    #         )
    #         output_token = out_token_count.input_tokens
    #         self.output_token += output_token
    #         return input_token + output_token
    #     else:
    #         try:
    #             enc = tiktoken.encoding_for_model(self.model)
    #         except KeyError:
    #             enc = tiktoken.get_encoding("cl100k_base")
    #         # if self.model == 'gpt-4o-mini':
    #         #     try:
    #         #         enc = tiktoken.encoding_for_model(self.model)
    #         #     except KeyError:
    #         #         enc = tiktoken.get_encoding("cl100k_base")
    #         # else:
    #         #     enc = tiktoken.encoding_for_model(self.model)
    #         # enc = tiktoken.encoding_for_model(self.model)
    #         # enc = tiktoken.get_encoding("o200k_base")
    #     in_tokens_num = len(enc.encode(in_text))
    #     out_tokens_num = len(enc.encode(out_text))
    #     self.input_token += in_tokens_num
    #     self.output_token += out_tokens_num
    #     return in_tokens_num + out_tokens_num

    # def calc_money(self, in_text, out_text):
    #     """money for gpt4"""
    #     if self.model == "gpt-4":
    #         return (self.calc_token(in_text) * 0.03 + self.calc_token(out_text) * 0.06) / 1000
    #     elif self.model == "gpt-3.5-turbo":
    #         return (self.calc_token(in_text) * 0.0015 + self.calc_token(out_text) * 0.002) / 1000
    #     elif self.model == "gpt-4-1106-preview" or self.model == "gpt-4-1106-vision-preview":
    #         return (self.calc_token(in_text) * 0.01 + self.calc_token(out_text) * 0.03) / 1000
    #     elif self.model == 'deepseek-chat':
    #         # input text: 0.14/1M, output text: 0.28/1M
    #         return (self.calc_token(in_text) * 0.14 + self.calc_token(out_text) * 0.28) / 1000000
    #     else:
    #         return 0 

    # def remove_html_tags(self, text):
    #     clean = re.compile('<.*?>')
    #     return re.sub(clean, '', text)
    
    # def _calculate_token_usage(self, token_usage):
    #     """
    #     "usage": {
    #         "completion_tokens": 0,
    #         "prompt_tokens": 0,
    #         "prompt_cache_hit_tokens": 0,
    #         "prompt_cache_miss_tokens": 0,
    #         "total_tokens": 0
    #     }
    #     """

    #     if "deepseek" in self.type:
    #         total_tokens = token_usage.total_tokens 
    #         completion_tokens = token_usage.completion_tokens
    #         prompt_tokens = token_usage.prompt_tokens
    #         prompt_cache_hit_tokens = token_usage.prompt_cache_hit_tokens
    #         prompt_cache_miss_tokens = token_usage.prompt_cache_miss_tokens
    #         current_time = datetime.now().strftime("%Y%m%d%H%M")
    #         if os.path.exists(os.path.join(self.usage_save_path, 'token_usage.txt')):
    #             with open(os.path.join(self.usage_save_path, 'token_usage.txt'), 'a') as f:
    #                 f.write(f"{current_time}, {total_tokens}, {completion_tokens}, {prompt_tokens}, {prompt_cache_hit_tokens}, {prompt_cache_miss_tokens}\n")
    #         else:
    #             with open(os.path.join(self.usage_save_path, 'token_usage.txt'), 'w') as f:
    #                 f.write(f"current_time, total_tokens, out_tokens, in_tokens, cache_hit_tokens, cache_miss_tokens\n")
    #                 f.write(f"{current_time}, {total_tokens}, {completion_tokens}, {prompt_tokens}, {prompt_cache_hit_tokens}, {prompt_cache_miss_tokens}\n")
    #     else:
    #         total_tokens = token_usage.total_tokens 
    #         completion_tokens = token_usage.completion_tokens
    #         prompt_tokens = token_usage.prompt_tokens
    #         current_time = datetime.now().strftime("%Y%m%d%H%M")
    #         if os.path.exists(os.path.join(self.usage_save_path, 'token_usage.txt')):
    #             with open(os.path.join(self.usage_save_path, 'token_usage.txt'), 'a') as f:
    #                 f.write(f"{current_time}, {total_tokens}, {completion_tokens}, {prompt_tokens}\n")
    #         else:
    #             with open(os.path.join(self.usage_save_path, 'token_usage.txt'), 'w') as f:
    #                 f.write(f"current_time, total_tokens, out_tokens, in_tokens\n")
    #                 f.write(f"{current_time}, {total_tokens}, {completion_tokens}, {prompt_tokens}\n")



    
    

if __name__ == '__main__':
    api_base = os.environ.get("GEMINI_API_BASE")
    api_key = os.environ.get("GEMINI_API_KEY")
    print(api_base)
    print(api_key)
    model_type = "gemini-2.5-flash-preview-05-20"
    model = GPT(api_base=api_base, api_key=api_key, model=model_type)
    prompt = "Hello, 1+1=?"
    response = model.get_GPT_response_json(prompt=prompt, json_format=True)
    print(response)
    


