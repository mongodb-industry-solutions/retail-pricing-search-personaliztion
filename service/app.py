from flask import Flask, request, jsonify
import json
import time
from flask_cors import CORS, cross_origin
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
import certifi

from sentence_transformers import SentenceTransformer, util

# Load the model
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')


app = Flask(__name__)
CORS(app)

# init kafka producer
KAFKA_SERVER = "localhost:9092"
producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)

# init mongo connection client
MONGO_CONN = 'mongodb+srv://<username>:<password>@retail-demo.2wqno.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(MONGO_CONN, tlsCAFile=certifi.where())

def q_obj(search_term):
    return [
        {
            '$search': {
                'index': 'default',
                'autocomplete': {
                    'query': search_term,
                    'path': 'query',
                    'fuzzy': {
                        'maxEdits': 2,
                        'prefixLength': 3
                    }
                }
            }
        }, {
            '$project': {
                'irscore': {
                    '$meta': 'textScore'
                },
                'query': 1,
                'count': 1,
                "_id": 0
            }
        }, {
            '$sort': {
                'irscore': -1,
                'count': -1
            }
        }, {
            '$limit': 10
        }
    ]

def get_search_query(search_term,qvec,vs=False):
    text_query ={"text": {
                    "query": search_term,
                    "path": {"wildcard":"*"},
                    "score": { "boost": {"value": 3 }}
                }}
    vector_query ={"knnBeta": {
                    "vector": qvec.tolist(),
                    "path": "vec",
                    "k": 100
                }}

    if vs:
        search_query = {
                    "$search": {
                        "index": "default",
                        "knnBeta": {
                            "vector": qvec.tolist(),
                            "path": "vec",
                            "k": 100,
                            "filter":{
                                "compound":{
                                    "must":[text_query]
                                }
                            }
                        }
                    }
                }
    else:
        search_query = {
            "$search": {
                "index": "default",
                "compound": {
                    "must":[text_query]
                }
            }
        }
    
    pipeline= [
                search_query,
                { "$limit": 20 },
                {"$project": {
                        "_id":0,
                        'irscore': {
                            '$meta': 'searchScore'
                        },
                        "title": 1,
                        "id": 1,
                        "mfg_brand_name": 1,
                        "pred_price": 1,
                        "price_elasticity": 1,
                        "discountedPrice": 1,
                        "link": 1,
                        "atp": 1,
                        "score": 1
                    }
                },
                {"$sort": {
                        "irscore": -1,
                        "score": -1,
                        "price_elasticity": 1
                    }
                }
            ]
    return pipeline

def get_featured_items(limit):
    return [
            {"$match":{
                "atp": 1,
                "articleType" : {"$in":["Tshirts", "Shirts", "Casual Shoes","Watches", "Handbags"]}
            }},
            {"$sort": {
                    "price_elasticity": 1,
                    "pred_price": 1,
                    "score": -1
                                   
                }
            },
            {"$limit": limit},
            {"$project": {
                    "_id":0,
                    "title": 1,
                    "id": 1,
                    "mfg_brand_name": 1,
                    "pred_price": 1,
                    "price_elasticity": 1,
                    "discountedPrice":1,
                    "atp": 1,
                    "score": 1,
                    "link": 1
                }
            }
    ]

@app.route('/retail/facet',methods=['GET', 'POST'])
def facet_query():
    search_term = request.args.get('query')
    vector_search= request.args.get('vecSearch')
    print(search_term)
    qvec = model.encode(search_term)
    text_query ={"text": {
                    "query": search_term,
                    "path": {"wildcard":"*"},
                    "score": { "boost": {"value": 3 }}
                }}
    vector_query ={"knnBeta": {
                    "vector": qvec.tolist(),
                    "path": "vec",
                    "k": 100
                }}

    if vector_search:
        compound_query = {"must": [vector_query],"should":[text_query]}
    else:
        compound_query = {"must":[text_query]}
    pipeline = [{"$searchMeta": { 
                "index": "default",
                "facet": {
                    "operator":{"compound": compound_query,},
                    "facets": {
                    "mfg_brand_name": {"type": "string", "path": "mfg_brand_name"},
                    "articleType": {"type": "string", "path": "articleType"},
                    }}}}]
    resp = client['search']['catalog_final_myn'].aggregate(pipeline)
    x = list(resp)
    return jsonify(x)

def get_atp_prd_cnt(pid):
    db = client['search']
    collection = db['atp_status_myn']
    print({"id": pid})
    return collection.find_one({"id": pid})['count']

@app.after_request
def add_header(response):
    response.headers['Content-Type'] = 'application/json'
    return response

@app.route('/retail/pushToCollection', methods=['POST'])
@cross_origin()
def kafkaProducer():		
    req = request.get_json()
    msg = req['message']
    topic_name = req['topic']
    if not type(msg) == list:
        msg = [msg]
    for ele in msg:
        json_payload = json.dumps(ele)
        json_payload = str.encode(json_payload)
        # update atp status for purchase events
        print(ele)
        if ("event_type" in ele) and (ele["event_type"] == "purchase"):
            atp = {}
            atp['id'] = str(ele['product_id'])
            if "count" not in ele:
                ele['count'] = 1
            atp['count'] = get_atp_prd_cnt(atp['id']) - ele['count']
            if atp['count'] > 0:
                atp['atp'] = 1
            else:
                atp['atp'] = 0
            atp_payload = json.dumps(atp)
            atp_payload = str.encode(atp_payload)
            producer.send("atp", atp_payload)

        producer.send(topic_name, json_payload)
        producer.flush()
        print("Sent to consumer")
    return jsonify(response={
        "message": f"{topic_name} is updated with the message", 
        "status": "Pass"})

@app.route('/retail/autocomplete', methods=['GET'])
@cross_origin()
def autocomplete():
    search_term = request.args.get('query')
    result = client['search']['autocomplete_myn'].aggregate(q_obj(search_term))
    resp = list(result)
    return jsonify(resp)

@app.route('/retail/search', methods=['GET'])
@cross_origin()
def search():
    search_term = request.args.get('query')
    sort_opt = request.args.get('sortOpt')
    vector_search= request.args.get('vecSearch')

    # sort_query = {"$sort":{"score": -1}}
    qvec = query_vector = model.encode(search_term)
    search_query = get_search_query(search_term,qvec,vs=vector_search)    
    # search_query += [sort_query]
    result = client['search']['catalog_final_myn'].aggregate(search_query)
    resp = list(result)
    return jsonify(resp)


@app.route('/retail/featured', methods=['GET'])
@cross_origin()
def featured():
    limit = request.args.get('topN')
    if not limit:
        limit = 20
    else:
        limit = int(limit)
    f_query = get_featured_items(limit)
    result = client['search']['catalog_final_myn'].aggregate(f_query)
    resp = list(result)
    return jsonify(resp)

@app.route('/retail/pid/<string:pid>', methods=['GET'])
@cross_origin()
def get_by_id(pid):
    result = client['search']['catalog_final_myn'].find_one({"id": pid}, {"_id":0})
    return jsonify(result)



# pdp endpoint product id *
# shorten title
# price
# payload for cart, view, checkout

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0',port = 3001)
    