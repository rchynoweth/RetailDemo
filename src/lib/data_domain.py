import random


class DataDomain():
  
  __instance = None

  @staticmethod
  def getInstance():
      """ Static access method. """
      if DataDomain.__instance == None:
          DataDomain()
      return DataDomain.__instance 
    
  
  def __init__(self):
    self.store_ids = [] 
    self.customer_ids = []
    
    
  def select_random_customer(self):
    return self.customer_ids[random.randint(0, len(self.customer_ids)-1)]

  
  def select_random_store(self):
    return self.store_ids[random.randint(0, len(self.store_ids)-1)]
  
  
  def add_customer(self, cid):
    self.customer_ids.append(cid)
    
  
  def add_store(self, sid):
    self.store_ids.append(sid)  
    
