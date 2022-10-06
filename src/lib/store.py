import random
import uuid 
from faker import Faker 
import datetime


class Store():
  
  def __init__(self):
    self.fake = Faker()
    self.store_ids = []
    
    
  def select_random_store(self):
    return self.store_ids[random.randint(0, len(self.store_ids)-1)]
    
  
  def create(self):
    """
    Creates a store 
    """
    sid = str(uuid.uuid4())

    data = {
      'store_id': sid,
      'manager': self.fake.name(),
      'created_date': str(datetime.datetime.utcnow()), 
      'created_by': 'system'
    }

    return data
  
  
