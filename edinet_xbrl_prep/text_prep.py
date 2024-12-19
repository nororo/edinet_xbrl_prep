

from pydantic import BaseModel, Field
from typing import Literal
import json
from typing import Annotated
from pydantic.functional_validators import BeforeValidator
StrOrNone = Annotated[str, BeforeValidator(lambda x: x or "")]

class Prompt(BaseModel):
    instruction: str = Field(..., title="Subscription-Key", description="API Key")
    example: str = Field(..., title="Example", description="Example")
    constraints_list: list[str] = Field(..., title="Constraints", description="Constraints")
    output_format: str = Field(..., title="Output Format", description="Output Format")
    
    def export(self,provided_text):
        constraint = "#### 注意事項"+"\n"+" * "+("\n"+" * ").join(self.constraints_list)
        system_prompt = self.instruction+"\n\n"+constraint+"\n\n"+self.output_format+"\n\n"+"#### 例"+"\n"+self.example
        user_prompt = "#### 文章"+"\n"+provided_text+"\n\n"+"#### 回答" +"\n"
        return system_prompt,user_prompt

        
    def export_sample(self,sample_text):
        system_prompt,user_prompt = self.export(sample_text)
        print("==== system_prompt ====")
        print(system_prompt)
        print("==== user_prompt ====")
        print(user_prompt)

from groq import Groq

class ResponseGenAI(BaseModel):
    output: StrOrNone = Field(..., title="Subscription-Key", description="API Key")
    input_token_size: int = Field(..., title="Input Token Size", description="Input Token Size")
    output_token_size: int = Field(..., title="Output Token Size", description="Output Token Size")
    status: Literal['success','failure'] = Field('failure', title="status", description="status")
    error_message: StrOrNone = Field(None, title="Error Message", description="Error Message")
    output_format_validation: Literal['success','failure'] = Field('success', title="status", description="status")
    output_format_validation_error_message: StrOrNone = Field(None, title="Error Message", description="Error Message")

    def output_json_validation(self)->bool:
        try:
            self.extract_output_json()
            return True
        except Exception as e:
            print(e)
            return False

    def extract_output_json(self)->list:
        text = self.output
        if len(text)==0:
            ValueError("text is empty")
        text=re.sub(r'\n', '',text)
        pattern = '{.*?}'
        text_json_list=re.findall(pattern, text)
        item_list=[]
        for text_json in text_json_list:
            tmp_dict=json.loads(text_json)
            item_list.append(tmp_dict)
        return item_list


class GroqAPI():
    def __init__(self,api_key:str):
        self.client = Groq(api_key=api_key,max_retries=2)

    def request(self,sys_prompt:str,usr_prompt:str,max_tokens=1024,temperature=0.6,model_name='llama-3.1-8b-instant')->dict:
        """
        messages: list of dict, role and content
        """
        messages = [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": usr_prompt},
            ]
        response_temp={'status':None,'output':None,'error_message':None,'input_token_size':None,'output_token_size':None}
        try:
            response = self.client.chat.completions.create(
                model=model_name,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature)
            response_temp['output'] = response.choices[0].message.content
            response_temp['input_token_size'] = response.usage.prompt_tokens
            response_temp['output_token_size'] = response.usage.completion_tokens
            response_temp['status'] = "success"
            return ResponseGenAI(**response_temp)
        except Exception as e:
            print(e)
            response_temp['error_message'] = str(e)
            response_temp['status'] = "failure"
            response_temp['input_token_size'] = 0
            response_temp['output_token_size'] = 0
            return ResponseGenAI(**response_temp)



import unicodedata
import string
import re

def htmldrop(text:str)->str:
    return re.sub(re.compile('<.*?>'), '', text)

def dropnumber(text:str)->str:
    """
    pattern = r'\d+'
    replacer = re.compile(pattern)
    result = replacer.sub('0', text)
    """
    # 連続した数字を0で置換
    replaced_text = re.sub(r'\d+', '', text)
    
    return replaced_text

def RtnDroper(text:str)->str:
    replaced_text=text.replace('\n\n','\n')
    replaced_text=replaced_text.replace('\n \n','\n')
    if replaced_text==text:
        return replaced_text
    else:
        return RtnDroper(replaced_text)
    
def preproc_nlp(text:str,drop_htmp:bool=False,drop_number:bool=False,reduce_return=False)->str:
    # drop html tag

    if drop_htmp:
        text = htmldrop(text)
    # unicode
    text = unicodedata.normalize("NFKC", text)
    # drop number
    if drop_number:
        text = dropnumber(text)
    # drop signature 1
    text = re.sub(re.compile("[!-/:-@[-`{-~]"), '', text)
    # drop signature 2
    text = re.sub(r'\(', '', text)
    text = text.replace('。','\n')
    # drop signature 3
    table = str.maketrans("", "", string.punctuation  + "◆■※")
    text = text.translate(table)
    # drop return (recursive)
    if reduce_return:
        text=RtnDroper(text)    
    
    return text