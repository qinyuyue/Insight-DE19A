# Insight-DE19A
## Inteseting Photo from Vistor's View 
## Business Value
When visitors go to a landmark spot, they may want to take good photo but just boring photo. This webpage can provide most recently popular scenery photos on instagram with same place. So now visitor may get new idea about their new photos.

## Data 
Source 1. Instagram API (only the data before April 2018)<br>
Source 2. European Cities 1M dataset (with geotags)

## Schema 
When user A is in place B, webpage will located him. With this location, app will generate a list of geotag used on instagram. The webpage will ask Instagram API to get recently 1 month image with these geotag. Then filter the scenry photos with specific hashtag. Display these image data to user. 

## Tech Stack
S3, Kafka, (maybe neo4j)

## Engineer Challenge 
heavy image transmission. Image Filter. Image classfication. 

## MVP 
A webpage display these filtted images. 
