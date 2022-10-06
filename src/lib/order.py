import random
import uuid 
import datetime
import time 


class Order():
  
  def __init__(self):
    self.order_sources = ['web', 'ios', 'android']
    self.order_types = ['pickup', 'delivery']
  
  def random_date(self):
    d = random.randint(1599485474, int(time.time()))
    return datetime.datetime.fromtimestamp(d).strftime('%Y-%m-%d %H:%M:%s')
  
  
  
  def create(self, cid, sid, products, order_source=None, order_type=None, product_min=2, product_max=20, qty_min=1, qty_max=10):
    """
    Produces a random set of products to be used in an orer
    
    :param products: is an object of the products class 
    :param product_min: integer. Min number of products in the basket
    :param product_max: integer. Max number of products in the basket
    :param qty_min: integer. Min quantity per product in the basket
    :param qty_max: integer. Max quantity per product in the basket
    """
    actions = ['viewed', 'added', 'removed']
    
    basket_items, customer_actions = [], []
    for i in range(1,random.randint(product_min,product_max)):
      qty = random.randint(qty_min, qty_max)
      product = products.get_random_product()
      pid = product.get('product_id')
      total = round(qty*product.get('price_per_unit'), 2)
      
      # VIEW 
      item = {'product_id': pid, 'qty': qty, 'total': total, 'action': 'viewed', 'time': time.time()}
      customer_actions.append(item)
      
      # ADD 
      if random.random() >= 0.10:
        item['time'] = time.time()
        item['action'] = 'added'
        customer_actions.append(item)
        basket_items.append({k: item.get(k) for k in ['product_id', 'qty', 'total']})
        # REMOVE --- i > 1 helps us avoid the possiblity of an empty basket
        if random.random() > 0.98 and i > 1: 
          _ = basket_items.pop() # remove basket item
          item['time'] = time.time()
          item['action'] = 'removed'
          customer_actions.append(item)
          # ADD again with new qty 
          if random.random() >= 0.60 and i > 1: 
            item['time'] = time.time()
            item['action'] = 'added'
            qty = random.randint(qty_min, qty_max)
            total = round(qty*product.get('price_per_unit'), 2)
            item['qty'] = qty
            item['total'] = total 
            customer_actions.append(item)
            basket_items.append({k: item.get(k) for k in ['product_id', 'qty', 'total']})
            
    order_data = {
      'order_id': str(uuid.uuid4()), 
      'customer_id': cid,
      'store_id': sid,
      'datetime': self.random_date(), 
      'order_type': order_type if order_type is not None else self.order_types[random.randint(0, len(self.order_types)-1)] ,
      'order_source': order_source if order_source is not None else self.order_sources[random.randint(0, len(self.order_sources)-1)] ,
      'basket': basket_items
    }
    
    action_data = {
      'order_id': str(uuid.uuid4()), 
      'customer_id': cid,
      'datetime': time.time(), 
      'actions': customer_actions
    }
    return order_data, action_data
  
  
