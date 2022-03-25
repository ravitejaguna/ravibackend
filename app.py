from datetime import datetime
from flask_cors import CORS, cross_origin
from flask import Flask, render_template, request, redirect, url_for,jsonify
app = Flask(__name__)

CORS(app, support_credentials=True, resources={r"/*": {"origins": "*"}})


#CORS(app, support_credentials=True, resources={r"/api/*": {"origins": "http://localhost:4900"}})

@app.route('/')
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def index():
   print('Request for index page received')
   return render_template('index.html')

@app.route('/home', methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def ReturnJSON():
    if(request.method == 'GET'):
        data = [
            {
            "Name" : "Raviteja",
            "Team" : "QM",
			"Role": "Dev"
        },
        {
            "Name" : "Shanmuga",
            "Team" : "QM",
			"Role": "Dev"
        }
        ]
  
        return jsonify(data)


   
@app.route('/print')
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def printMsg():
    app.logger.warning('testing warning log')
    app.logger.error('testing error log')
    app.logger.info('testing info log')
    return "Check your console"   


@app.route('/hello', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('hello.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))


if __name__ == '__main__':
   app.run()