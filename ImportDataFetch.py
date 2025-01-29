import duckdb 
#import json                # use as alternative in case of other database been use
#import pandas as pd        # use as alternative in case of other database been use
#import gzip                # use as alternative in case of other database been use

# if wanting to loop through json instead of utlizing duckdb not been used in this excerice
"""def parseitemlist(file):
        # Open json files
        ReceiptItemList = []            # list for rewardsReceiptItemList
        with gzip.open(file, 'r') as file:
                for line in file:
                        # load each line at a time to account for schema diffrences and multi line
                        data = json.loads(line) # load into dict data type

                        # keep only _id (used to join into recipt later on) and itemlist
                        subdata = ({k: v for k, v in data.items() if k in ('_id', 'rewardsReceiptItemList')})

                        if subdata.get('rewardsReceiptItemList',False):                 # if key exist then load into list
                                for item in subdata['rewardsReceiptItemList']:          # loop through item list
                                        item['recpt_id'] = subdata['_id']['$oid']       # attacht recpt_id to each item line
                                        ReceiptItemList.append(item)                    # add to list
                
        return pd.DataFrame(ReceiptItemList)"""

#Insert into receipts table
duckdb.sql("""CREATE TABLE receipts as 
            Select      _id."$oid"                              AS recpt_id
                    ,   CAST(bonusPointsEarned AS INT)          AS bonusPointsEarned
                    ,   bonusPointsEarnedReason
                    ,   epoch_ms(createDate."$date")            AS CreateDate   
                    ,   epoch_ms(dateScanned."$date")           AS dateScanned
                    ,   epoch_ms(finishedDate."$date")          AS finishedDate
                    ,   epoch_ms(modifyDate."$date")            AS modifyDate
                    ,   epoch_ms(pointsAwardedDate."$date")     AS pointsAwardedDate
                    ,   CAST(pointsEarned AS INT)               AS pointsEarned
                    ,   epoch_ms(purchaseDate."$date")          AS purchaseDate
                    ,   CAST(purchasedItemCount AS INT)         AS purchasedItemCount
                    ,   rewardsReceiptStatus
                    ,   CAST(totalSpent AS DECIMAL(18,2))       AS totalSpent
                    ,   userId
            FROM    'receipts.json.gz'  """)



#insert into rewardItemList table where rewardsReceiptItemList is not null
# we are using unnst to extract all nested dict and list from json into its indivual row
duckdb.sql("""  CREATE TABLE rewardItemList AS
            WITH 
            preWork As (
            SELECT  _id."$oid"   AS  recpt_id
                ,   unnest(rewardsReceiptItemList, recursive := true) 
            FROM   'receipts.json.gz' 
            WHERE  rewardsReceiptItemList is not null)
           
           SELECT   recpt_id 
                ,   barcode  
                ,   description  
                ,   CAST(finalPrice AS DECIMAL(18,2))       AS finalPrice                        
                ,   CAST(itemPrice AS  DECIMAL(18,2))       AS itemPrice
                ,   needsFetchReview 
                ,   CAST(partnerItemId  AS INT)             AS partnerItemId
                ,   preventTargetGapPoints  
                ,   CAST(quantityPurchased AS INT)          AS quantityPurchased
                ,   userFlaggedBarcode  
                ,   userFlaggedNewItem  
                ,   CAST(userFlaggedPrice AS DECIMAL(18,2)) AS userFlaggedPrice  
                ,   CAST(userFlaggedQuantity AS INT)        AS userFlaggedQuantity          
                ,   needsFetchReviewReason  
                ,   pointsNotAwardedReason  
                ,   pointsPayerId  
                ,   rewardsGroup  
                ,   rewardsProductPartnerId  
                ,   userFlaggedDescription  
                ,   originalMetaBriteBarcode  
                ,   originalMetaBriteDescription  
                ,   brandCode  
                ,   competitorRewardsGroup  
                ,   CAST(discountedItemPrice AS DECIMAL(18,2))  AS discountedItemPrice
                ,   originalReceiptItemText 
                ,   itemNumber  
                ,   CAST(originalMetaBriteQuantityPurchased AS INT) AS originalMetaBriteQuantityPurchased
                ,   CAST(pointsEarned  AS FLOAT)            AS pointsEarned                      
                ,   CAST(targetPrice AS INT)                AS targetPrice
                ,   competitiveProduct  
                ,   CAST(originalFinalPrice AS DECIMAL(18,2)) AS originalFinalPrice
                ,   CAST(originalMetaBriteItemPrice AS DECIMAL(18,2)) AS originalMetaBriteItemPrice
                ,   deleted  
                ,   CAST(priceAfterCoupon AS DECIMAL(18,2)) AS priceAfterCoupon                
                ,   metabriteCampaignId
           
           FROM     preWork""")


duckdb.sql("""  CREATE TABLE brands AS
                SELECT  _id."$oid"          brand_id
                    ,   barcode 
                    ,   category
                    ,   categoryCode
                    ,   cpg."$id"."$oid"    cpg_id
                    ,   cpg."$ref"          cpg_ref
                    ,   name                brandName
                    ,   topBrand    
                    ,   brandCode
           
                FROM   'brands.json.gz' """)

# must do distinct to get rid off any duplicated within users
duckdb.sql("""  CREATE TABLE users AS
                SELECT  DISTINCT
                        _id."$oid"     user_id
                    ,   active         
                    ,   epoch_ms(createdDate."$date") createdDate 
                    ,   epoch_ms(lastLogin."$date")   lastLogin
                    ,   role
                    ,   signUpSource
                    ,   state
                FROM 'users.json'""")

# CHECK UNIQUE PRIMARY KEY
print(duckdb.sql('''WITH
                    SUB AS (SELECT brand_id, count(*) cnt FROM brands GROUP BY 1 HAVING cnt > 1)
                    SELECT      COUNT(*)
                    FROM        brands              MN
                    INNER JOIN  SUB                 SUB
                            ON  MN.brand_id       = SUB.brand_id'''))
# CHECK UNIQUE PRIMARY KEY
print(duckdb.sql('''WITH
                    SUB AS (SELECT user_id, count(*) cnt FROM users GROUP BY 1 HAVING cnt > 1)
                    SELECT      COUNT(*)
                    FROM        users              MN
                    INNER JOIN  SUB                SUB
                            ON  MN.user_id       = SUB.user_id'''))
# CHECK UNIQUE PRIMARY KEY
print(duckdb.sql('''WITH
                    SUB AS (SELECT recpt_id, count(*) cnt FROM receipts GROUP BY 1 HAVING cnt > 1)
                    SELECT      COUNT(*)
                    FROM        receipts           MN
                    INNER JOIN  SUB                SUB
                            ON  MN.recpt_id      = SUB.recpt_id'''))
# CHECK UNIQUE PRIMARY KEY
print(duckdb.sql('''WITH
                    SUB AS (SELECT recpt_id ,partnerItemId, count(*)cnt FROM rewardItemList GROUP BY 1,2 having cnt > 1)
                    SELECT      count(*)
                    FROM        rewardItemList     MN
                    INNER JOIN  SUB                SUB
                            ON  MN.recpt_id      = SUB.recpt_id
                           AND  MN.partnerItemId = SUB.partnerItemId
                     '''))

# Validate to se diffrences between rows to find unique key combination
print(duckdb.sql('''WITH
                    SUB AS (SELECT recpt_id ,barcode, count(*)cnt FROM rewardItemList GROUP BY 1,2 having cnt > 1)
                    SELECT      *
                    FROM        rewardItemList     MN
                    INNER JOIN  SUB                SUB
                            ON  MN.recpt_id      = SUB.recpt_id
                           AND  MN.barcode       = SUB.barcode'''))
#CHECK if brandcode and barcode are unique
print(duckdb.sql('''SELECT      brandcode, barcode,COUNT(*)
                    FROM        brands              MN
                    group by    1,2
                    having      count(*) > 1
                   '''))
