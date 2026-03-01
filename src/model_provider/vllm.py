import re
import json
import torch
from huggingface_hub import login
from transformers import LlamaForCausalLM, LlamaTokenizer, AutoModelForCausalLM, AutoTokenizer


class VLLM:
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