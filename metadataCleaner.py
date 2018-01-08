import json
import csv
import gzip

def metadataCleaner():
    product_ids = []
    product_ids_file = open("distinct_asin.txt", 'r')
    for product_id in product_ids_file:
        product_ids.append(product_id.strip())
   
    chunks = [product_ids[x:x+100] for x in range(0, len(product_ids), 100)]      
    outputFile  = open("metadata_products.json", 'a')
    
    i =0
    for chunk in chunks:
        print "Processing Chunk ", i
        record_extractor(outputFile, chunk)
        i = i +1
    print "Done"    
    
    
def record_extractor(outputFile, product_ids):  
    records = []
    json_file = open('metadata.json', 'r')
    for line in json_file:
        product_id = line[10:20]
        
        if product_id in product_ids:
            records.append(line)
    print "Done"
    print len(records)
    
    #outputFile  = open("metadata_products.json", 'wb')
    for record in records:
        outputFile.write(record)  
     
    return


metadataCleaner()
    
    