import random
import uuid 
from faker import Faker 
import datetime
import pandas as pd
import numpy as np




class Products():
  
  def __init__(self, pdf):
    self.pdf = pdf 
    assert ('title' and 'product_id' )in self.pdf.columns
    
    self.pdf['price_per_unit'] = [round(random.uniform(1,200), 2) for i in self.pdf.index]
    self.pdf['vendor_id'] = np.random.randint(1, 3000, self.pdf.shape[0])
    self.product_dict = self.pdf.to_dict('records')
    
    
  
  def get_random_product(self):
    return self.pdf.iloc[[random.randint(0,len(self.pdf)-1)]].to_dict('records')[0]
  
  
  def get_products(self):
    return self.pdf
  
  
  def get_products_dict(self):
    return self.product_dict
