import random
import uuid 
from faker import Faker 
import datetime
import time 


class Order():
  
  def __init__(self):
    self.fake = Faker()
    self.order_sources = ['web', 'ios', 'android']
    self.order_types = ['pickup', 'delivery']
  
  def random_date(self):
    d = random.randint(1599485474, int(time.time()))
    return datetime.datetime.fromtimestamp(d).strftime('%Y-%m-%d %H:%M:%s')
  
  
  
  def create_order(self, cid, sid, products, order_source=None, order_type=None, product_min=2, product_max=20, qty_min=1, qty_max=10):
    """
    Produces a random set of products to be used in an orer
    
    :param products: is an object of the products class 
    :param product_min: integer. Min number of products in the basket
    :param product_max: integer. Max number of products in the basket
    :param qty_min: integer. Min quantity per product in the basket
    :param qty_max: integer. Max quantity per product in the basket
    """
    items = []
    for i in range(1,random.randint(product_min,product_max)):
      qty = random.randint(qty_min, qty_max)
      row = products.get_random_product()
      items.append({'product_id': row.get('product_id'), 'qty': qty, 'total': round(qty*row.get('price_per_unit'), 2)})
    
    
    d = {
      'order_id': str(uuid.uuid4()), 
      'customer_id': cid,
      'store_id': sid,
      'datetime': self.random_date(), 
      'order_type': order_type if order_type is not None else self.order_types[random.randint(0, len(self.order_types)-1)] ,
      'order_source': order_source if order_source is not None else self.order_sources[random.randint(0, len(self.order_sources)-1)] ,
      'basket': items
    }
    return d
  
  
