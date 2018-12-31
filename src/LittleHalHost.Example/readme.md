A minimal example here simply appends a few messages to an InMemoryStreamStore and exposes that over HTTP.  
You can  get all messages http://localhost:5050/stream?d=f&m=20&p=0&e=1 
 or  get messages from known stream   http://localhost:5050/streams/cbf68be34d9547eb9b4a390fd2aa417b?d=f&m=20&p=0&e=1    
 This will fetch the first page worth of messages (_embedded) and have a links collection with a next link you can follow to fetch the next page.  
 Supported parameters:  
 'd', readDirection, possible values F/B (Forward/Backward)  
'p',  position  Inclusive   
'm' maxCount  
'e', embedPayload -possible values 0/1  

See example of response  in ResponseExample.json


