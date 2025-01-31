# @TIME : 5/6/23 3:53 PM
# @AUTHOR : LZDH
# @TIME : 9/5/23 4:07 PM
# @AUTHOR : LZDH
import random
import pandas as pd
import re
# import openai
import zss
import ast
import time
from rewriter import *
from get_query_meta import *
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import sys

# Set PATHs
PATH_TO_SENTEVAL = './SentEval'
PATH_TO_DATA = './SentEval/data'
# Import SentEval
sys.path.insert(0, PATH_TO_SENTEVAL)
import senteval
from senteval.utils import cosine
from encoder import *
from models import QueryformerForCL
from openai import OpenAI
# import tiktoken
client = OpenAI(
    # This is the default and can be omitted
    api_key="your_openai_api_key"
)
os.environ["TOKENIZERS_PARALLELISM"] = "false"
pre_lang_model = SentenceTransformer('all-MiniLM-L6-v2')

model = QueryformerForCL()
model_name = 'tpch'
checkpoint = torch.load('simcse_models/' + model_name + '/pytorch_model.bin', map_location=torch.device('cpu'))
model.load_state_dict(checkpoint, strict=False)
