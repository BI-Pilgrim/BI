from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, create_engine, DateTime, Text, JSON
from sqlalchemy_bigquery import STRUCT

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
class GooglePlayRatingsPriv(Base):
    __tablename__ = 'google_play_ratings_priv'

    reviewId = Column(String, primary_key=True)
    authorName = Column(String)
    userComment_text = Column(String)
    userComment_lastModified_seconds = Column(DateTime(False))
    userComment_lastModified_nanos = Column(Integer)
    userComment_starRating = Column(Integer)
    userComment_reviewerLanguage = Column(String)
    userComment_device = Column(String)
    userComment_androidOsVersion = Column(Integer)
    userComment_appVersionCode = Column(Integer)
    userComment_appVersionName = Column(String)
    userComment_thumbsUpCount = Column(Integer)
    userComment_thumbsDownCount = Column(Integer)
    userComment_deviceMetadata = Column(String)
    developerComment_text = Column(String)
    developerComment_lastModified_seconds = Column(DateTime(False))
    developerComment_lastModified_nanos = Column(Integer)
    ee_extracted_at = Column(DateTime(True))

"""
{
  "reviewId": "11223344",
  "authorName": "John Doe",
  "comments": [
    {
      "userComment": {
        "text": "I love using this app!",
        "lastModified": {
          "seconds": "141582134",
          "nanos": 213000000
        },
        "starRating": 5,
        "reviewerLanguage": "en",
        "device": "trltecan",
        "androidOsVersion": 21,
        "appVersionCode": 12345,
        "appVersionName": "1.2.3",
        "thumbsUpCount": 10,
        "thumbsDownCount": 3,
        "deviceMetadata": {
          "productName": "E5333 (Xperia™ C4 Dual)",
          "manufacturer": "Sony",
          "deviceClass": "phone",
          "screenWidthPx": 1080,
          "screenHeightPx": 1920,
          "nativePlatform": "armeabi-v7a,armeabi,arm64-v8a",
          "screenDensityDpi": 480,
          "glEsVersion": 196608,
          "cpuModel": "MT6752",
          "cpuMake": "Mediatek",
          "ramMb": 2048
        }
      }
    },
    {
      "developerComment": {
        "text": "That's great to hear!",
        "lastModified": {
          "seconds": "1423101467",
          "nanos": 813000000
        }
      }
    }
  ]
}
"""

