import random
import uuid 
from faker import Faker 
import datetime


class Customer():
  
  def __init__(self):
    self.fake = Faker()
    
  
  def create_customer(self):
    """
    Creates a customer 
    """
    cid = str(uuid.uuid4())
    is_mem = 1 if random.random() >= 0.4 else 0
    
    data = {
      'customer_id': cid,
      'first_name': self.fake.first_name(),
      'last_name': self.fake.last_name(), 
      'is_member': is_mem,
      'member_number': self.fake.msisdn()[3:] if is_mem == 1 else None,
      'created_date': str(datetime.datetime.utcnow()), 
      'created_by': 'system'
    }
    
    return data
  
