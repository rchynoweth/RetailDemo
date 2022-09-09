import random
import uuid 
from faker import Faker 
import datetime


class Products():
  
  def __init__(self, pdf):
    self.pdf = pdf 
    assert ('title' and 'product_id' )in self.pdf.columns
    self.pdf['price_per_unit'] = [round(random.uniform(1,200), 2) for i in self.pdf.index]
    
    self.product_dict = self.pdf.to_dict('records')
    
  
  def get_random_product(self):
    return self.pdf.iloc[[random.randint(0,len(self.pdf)-1)]].to_dict('records')[0]
  
  
  def get_products(self):
    return self.pdf
  
  
  def get_products_dict(self):
    return self.product_dict
