import random
import uuid 
from faker import Faker 
import datetime


class Store():
  
  def __init__(self):
    self.fake = Faker()
    
  
  def create_store(self):
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
  
  
