from openai import OpenAI, APIError
import os
import re
import sys
import json
import time
import random
import datetime
import tiktoken
import transformers
import torch
import anthropic
from huggingface_hub import login
from transformers import LlamaForCausalLM, LlamaTokenizer, AutoModelForCausalLM, AutoTokenizer
from utils.misc import extract_json_knob_settings
from model_provider.llm import LLM
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

class Anthropic(LLM):
    def __init__(self, api_base, api_key, model_type="claude-3.7"):
        super.__init__(api_base=api_base, api_key=api_key, model_type=model_type)
        model_id_mapping = {
            "claude-3.7": "claude-3-7",
            "claude4-100k": "claude-4-100k"
        }
        self.model = model_id_mapping.get(model_type, "claude-3.7")
        self.money = 0
        self.token = 0
        self.input_token = 0
        self.output_token = 0
        self.cur_token = 0
        self.cur_money = 0

        self.current_run_usage = None

    def invoke_api(self, system_prompt, prompt, output_format, max_tokens=1000, n=3, log=None):
        if n <= 0:
            print("Call API failure.")
            exit()
        
        client = anthropic.Anthropic(
            api_key=self.api_key
        )

        try:
            if output_format:
                # Make a simple request
                response = client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    temperature=0.5,
                    system=system_prompt,
                    messages=[
                        {"role": "user", "content": f"{prompt}"}
                    ]
                )
                # print(response)
                ans = response.content[0].text
                if log:
                    log.info(f"Antropic raw response: {response}")
                completion = extract_json_knob_settings(ans) # Convert to json object
            else:
                # Make a simple request
                response = client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    temperature=1,
                    messages=[
                        {"role": "user", "content": f"{prompt}"}
                    ]
                )
                completion = response.content[0].text
                if log:
                    log.info(f"Antropic raw response: {response}")
        except APIError as e:
            print("Call API fail:", e)
            exit()
        except Exception as e:
            print("Response:", response)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(f"Exception Type: {exc_type.__name__}")
            print(f"Exception Message: {str(e)}")
            print(f"Occurred at Line: {exc_tb.tb_lineno}")
            print("Sleeping...")
            time.sleep(random.randint(30, 40))
            print("retry.")
            return self.get_GPT_response_json(prompt, output_format, max_tokens=max_tokens, n=n-1)
        return completion

    def _update_current_run_usage(self, response):
        self.output_token += response.completion_token
        self.input_token += response.prompt_token
        self.token += response.total_token
        
        if self.current_run_usage is None:
            self.current_run_uasge = {
                "input_token": response.prompt_token,
                "output_token": response.completion_token,
                "total": response.total_token
            }
        else:
            self.current_run_usage.update({
                "input_token": response.prompt_token,
                "output_token": response.completion_token,
                "total": response.total_token
            })

    @property
    def current_run_usage(self):
        if self.current_run_usage is None:
            return {
                "input_token": 0,
                "output_token": 0,
                "total": 0
            }
        return self.current_run_usage

    def calc_token(self, in_text, out_text=""):
        if isinstance(in_text, dict):
            in_text = json.dumps(in_text)
        
        if isinstance(out_text, dict):
            out_text = json.dumps(out_text)
            
        if self.model == 'deepseek-chat':
            chat_tokenizer_dir = "./src/knowledge_handler/deepseek_v3_tokenizer"
            enc =  transformers.AutoTokenizer.from_pretrained( 
                    chat_tokenizer_dir, trust_remote_code=True
                    )
        elif self.model.startswith('claude'):
            client = anthropic.Anthropic()
            
            # Use the count_tokens method
            in_token_count = client.messages.count_tokens(
                model=self.model,
                messages=[
                    {"role": "user", "content": in_text}
                ]
            )
            input_token = in_token_count.input_tokens
            self.input_token += input_token

            out_token_count = client.messages.count_tokens(
                model=self.model,
                messages=[
                    {"role": "user", "content": in_text}
                ]
            )
            output_token = out_token_count.input_tokens
            self.output_token += output_token
            return input_token + output_token
        else:
            try:
                enc = tiktoken.encoding_for_model(self.model)
            except KeyError:
                enc = tiktoken.get_encoding("cl100k_base")
            # if self.model == 'gpt-4o-mini':
            #     try:
            #         enc = tiktoken.encoding_for_model(self.model)
            #     except KeyError:
            #         enc = tiktoken.get_encoding("cl100k_base")
            # else:
            #     enc = tiktoken.encoding_for_model(self.model)
            # enc = tiktoken.encoding_for_model(self.model)
            # enc = tiktoken.get_encoding("o200k_base")
        in_tokens_num = len(enc.encode(in_text))
        out_tokens_num = len(enc.encode(out_text))
        self.input_token += in_tokens_num
        self.output_token += out_tokens_num
        return in_tokens_num + out_tokens_num

    def calc_money(self, in_text, out_text):
        """money for gpt4"""
        if self.model == "gpt-4":
            return (self.calc_token(in_text) * 0.03 + self.calc_token(out_text) * 0.06) / 1000
        elif self.model == "gpt-3.5-turbo":
            return (self.calc_token(in_text) * 0.0015 + self.calc_token(out_text) * 0.002) / 1000
        elif self.model == "gpt-4-1106-preview" or self.model == "gpt-4-1106-vision-preview":
            return (self.calc_token(in_text) * 0.01 + self.calc_token(out_text) * 0.03) / 1000
        elif self.model == 'deepseek-chat':
            # input text: 0.14/1M, output text: 0.28/1M
            return (self.calc_token(in_text) * 0.14 + self.calc_token(out_text) * 0.28) / 1000000
        else:
            return 0 

    def remove_html_tags(self, text):
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)
    
    def _calculate_token_usage(self, token_usage):
        """
        "usage": {
            "completion_tokens": 0,
            "prompt_tokens": 0,
            "prompt_cache_hit_tokens": 0,
            "prompt_cache_miss_tokens": 0,
            "total_tokens": 0
        }
        """

        if "deepseek" in self.type:
            total_tokens = token_usage.total_tokens 
            completion_tokens = token_usage.completion_tokens
            prompt_tokens = token_usage.prompt_tokens
            prompt_cache_hit_tokens = token_usage.prompt_cache_hit_tokens
            prompt_cache_miss_tokens = token_usage.prompt_cache_miss_tokens
            current_time = datetime.now().strftime("%Y%m%d%H%M")
            if os.path.exists(os.path.join(self.usage_save_path, 'token_usage.txt')):
                with open(os.path.join(self.usage_save_path, 'token_usage.txt'), 'a') as f:
                    f.write(f"{current_time}, {total_tokens}, {completion_tokens}, {prompt_tokens}, {prompt_cache_hit_tokens}, {prompt_cache_miss_tokens}\n")
            else:
                with open(os.path.join(self.usage_save_path, 'token_usage.txt'), 'w') as f:
                    f.write(f"current_time, total_tokens, out_tokens, in_tokens, cache_hit_tokens, cache_miss_tokens\n")
                    f.write(f"{current_time}, {total_tokens}, {completion_tokens}, {prompt_tokens}, {prompt_cache_hit_tokens}, {prompt_cache_miss_tokens}\n")
        else:
            total_tokens = token_usage.total_tokens 
            completion_tokens = token_usage.completion_tokens
            prompt_tokens = token_usage.prompt_tokens
            current_time = datetime.now().strftime("%Y%m%d%H%M")
            if os.path.exists(os.path.join(self.usage_save_path, 'token_usage.txt')):
                with open(os.path.join(self.usage_save_path, 'token_usage.txt'), 'a') as f:
                    f.write(f"{current_time}, {total_tokens}, {completion_tokens}, {prompt_tokens}\n")
            else:
                with open(os.path.join(self.usage_save_path, 'token_usage.txt'), 'w') as f:
                    f.write(f"current_time, total_tokens, out_tokens, in_tokens\n")
                    f.write(f"{current_time}, {total_tokens}, {completion_tokens}, {prompt_tokens}\n")


class LLM:
    def __init__(self, access_token, model='llama3-8b'):
        self.money = 0
        self.token = 0
        self.cur_token = 0
        self.cur_money = 0
        self.model_ids = {
            'llama3-8b': "meta-llama/Meta-Llama-3-8B-Instruct"
        }
        self._load_LLM_model(access_token=access_token, model=model)

    def _load_LLM_model(self, access_token, model):
        login(access_token)
        model_id = self.model_ids[model.lower()]
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_id,
        )
        self.llm = AutoModelForCausalLM.from_pretrained(
            model_id,
            torch_dtype=torch.float16,
            device_map="auto",
            attn_implementation="flash_attention_2"
        )
        # Set pad_token to eos_token
        self.tokenizer.pad_token = self.tokenizer.eos_token
        self.llm.config.pad_token_id = self.tokenizer.pad_token_id
    
    def get_GPT_response_json(self, prompt, json_format=True, n=3):
        if n <= 0:
            print("Fail to get response.")
            exit()

        if json_format:
            messages = [
                {"role": "system", "content": "You should output JSON."},
                {"role": "user", "content": f"{prompt}"},
            ]
            temperature = 0.5
        else:
            messages = [
                {"role": "user", "content": f"{prompt}"},
            ]
            temperature = 1
        
        input_ids = self.tokenizer.apply_chat_template(
            messages,
            add_generation_prompt=True,
            return_tensors="pt",
            padding=True
        ).to(self.llm.device)

        attention_mask = (input_ids != self.tokenizer.pad_token_id).long()

        terminators = [
            self.tokenizer.eos_token_id,
            self.tokenizer.convert_tokens_to_ids("<|eot_id|>")
        ]
        try:
            with torch.no_grad():
                outputs = self.llm.generate(
                    input_ids,
                    attention_mask=attention_mask,
                    eos_token_id=terminators,
                    temperature=temperature,
                    use_cache=False
                )
            response_tokens = outputs[0][input_ids.shape[-1]:]
            print(f"input tokens: {len(input_ids[0])}")
            print(f"output tokens: {len(response_tokens)}")
            self.cur_token = len(response_tokens) + len(input_ids[0])
            response = self.tokenizer.decode(response_tokens, skip_special_tokens=True)
            print("response:", response)
            if json_format:
                json_response = self.extract_json_from_response(response)
                if json_response is None:
                    return self.get_GPT_response_json(prompt=prompt, json_format=json_format, n=n-1)
                else:
                    return json_response
            return response
        except Exception as e:
            print(f"LLM error: {e}")
            print(f"The Input token length: {len(input_ids[0])}")
            exit()
        finally:
            torch.cuda.empty_cache()

    def calc_token(self):
        return self.cur_token

    def calc_money(self):
        return 0

    def remove_html_tags(self, text):
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)
    
    def extract_json_from_response(self, text):
        # Regex to find the JSON block between ```json ... ```
        try:
            json_data = json.loads(text) # test whether the response is pure json
            return json_data
        except: # if the response mix up the text and the json, extract the json
            json_block = re.search(r"```(.*?)```", text, re.DOTALL)
            if json_block:
                json_text = json_block.group(1).strip()  # Extract the JSON text inside ```json``` block
                try:
                    # Parse the JSON text
                    json_data = json.loads(json_text)
                    return json_data
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    return None
            else:
                # If no backtick block, attempt to find balanced curly braces
                start = text.find('{')
                if start == -1:
                    return None

                brace_count = 0
                for i in range(start, len(text)):
                    if text[i] == '{':
                        brace_count += 1
                    elif text[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            json_str = text[start:i+1]
                            try:
                                # Verify it's valid JSON
                                json_data = json.loads(json_str)
                                return json_data
                            except json.JSONDecodeError:
                                return None
                print("No JSON block found in the text.")
                return None
    
    

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
    


