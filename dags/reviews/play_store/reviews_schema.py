from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

"""
{'reviewId': 'e6efd98c-1e18-41fb-b5fe-6dc0140914d9',
  'userName': 'Kelvan Siew',
  'userImage': 'https://play-lh.googleusercontent.com/a/ACg8ocI0uHG5DNjgSKFcC1Sb2QNz8YSQnqKZcVD3xWP4uhSRmmfO=mo',
  'content': 'The penguins keep being more n more penguins r cute :D',
  'score': 5,
  'thumbsUpCount': 0,
  'reviewCreatedVersion': '1.79.0',
  'at': datetime.datetime(2025, 1, 12, 11, 36, 23),
  'replyContent': None,
  'repliedAt': None,
  'appVersion': '1.79.0'}
"""
from sqlalchemy import Column, Integer, String, Date, create_engine, DateTime, Text

class GooglePlayRatings(Base):
    __tablename__ = 'google_play_ratings'

    review_id = Column(String(255), primary_key=True)
    user_name = Column(String(255), nullable=True)
    user_image = Column(String(255), nullable=True)
    content = Column(Text, nullable=True) # Text is used for large text
    score = Column(Integer, nullable=True)
    thumbs_up_count = Column(Integer, nullable=True)
    review_created_version = Column(String(255), nullable=True)
    review_given_at = Column(DateTime, nullable=True)
    reply_content = Column(String(255), nullable=True)
    replied_at = Column(DateTime, nullable=True)
    app_version = Column(String(255), nullable=True)
    ee_extracted_at = Column(DateTime(True))

# class GooglePlayRatingsMetadata(Base):
#     __tablename__ = 'google_play_ratings_metadata'

#     last_review_created_on = Column(DateTime, nullable=False)
#     last_synced_on = Column(DateTime, nullable=False)


