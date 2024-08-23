from flask import Flask,jsonify,request
import pandas as pd
import requests
app=Flask(__name__)
data=pd.read_csv('./new_court_data.csv')
api_key='basickey'
@app.route('/',methods=['GET'])
def basic():
    return 'first api using flask',200
@app.route('/get_data',methods=['GET'])
def get_data():
    # if request.headers.get('Authorization') == f'Bearer {api_key}':
    key = request.args.get('api_key')
    if key== api_key:
        return jsonify(data.to_dict(orient='records'))
    else:
        return 'No Authorization'
if __name__ == '__main__':
    app.run(debug=True)
        