from flask import Flask, request
import psycopg2,json,requests
import webdav.client as wc
import boto3

try:
        connection = psycopg2.connect(user="usuario",
                password = "contrasenia",
                host = "host",
                port = "puerto",
                database = "cam")
        connection.autocommit = True
        cursor = connection.cursor()
except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while connecting to PostgreSQL", error)
app = Flask(__name__)

@app.route('/connecttodrive',methods = ['POST'])
def api_conectdrive():
    if request.headers['Content-Type'] == 'application/json':
        existe = 0
        datos = json.loads(request.data)
        session = requests.Session()
        session.auth = (datos['token_correo'],datos['token_contrasena'])
        verb = 'PROPFIND'
        body = '<?xml version="1.0" encoding="utf-8" ?>'
        body += '<D:propfind xmlns:D="DAV:">'
        body += '<D:allprop/></D:propfind>'
        headers = {'Depth': '1'}
        requestpath = datos['instancia']+'remote.php/webdav'
        response = session.request(verb, requestpath, data=body, headers=headers)
        print (response.text)
        free = response.text.split("<d:quota-available-bytes>")[1].split("</d:quota-available-bytes>")[0]
        optionswebdav = {
		 'webdav_hostname': requestpath,
		 'webdav_login':    datos["token_correo"],
		 'webdav_password': datos["token_contrasena"]
	}
        clientwebdav = wc.Client(optionswebdav)
        listacarpetas = clientwebdav.list()
        print (listacarpetas)
        verb = 'MKCOL'
        headers = {}
        body = ''
        requestpath = datos['instancia']+'remote.php/webdav/Carpeta'
        response = session.request(verb, requestpath, data=body, headers=headers)
        mb = float(free)/1000000
        horastotal = mb/200
        horaspcamara = int(horastotal/len(datos['camaras']))
        for i in datos['camaras']:
                existe = False
                cursor.execute('select * from tabla where id_camara=%s'%(i[0]))
                existe = cursor.fetchone()
                print(existe)
                if existe:
                        cursor.execute('update tabla set token=\'%s\', pswtoken=\'%s\', hrs_total=%s, instancia=\'%s\' where id_camara=%s;'%(datos["token_correo"],
																	datos["token_contrasena"],
																	horaspcamara,
																	datos["instancia"],
																	i[0]))
                else:
                        cursor.execute('insert into tabla(id_camara,token,pswtoken,hrs_total,hrs_ocupadas,bandera_web,bandera_servervideo,instancia,nserie,cloud) values(%s,\'%s\',\'%s\',%s,%s,%s,%s,\'%s\',\'%s\',\'claro\');'%(i[0],
																	datos["token_correo"],
																	datos["token_contrasena"],
																	horaspcamara,
																	0,
																	True,
																	True,
																	datos["instancia"],
																	i[1]))
        return "Exitoso"



@app.route('/datosalmacenamiento',methods=['POST'])
def api_getdatosalamacenamiento():
    if request.headers['Content-Type'] == 'application/json':
        respuesta_json = {}
        datos = json.loads(request.data)
        session = requests.Session()
        session.auth = (datos['token_correo'],datos['token_contrasena'])
        verb = 'PROPFIND'
        body = '<?xml version="1.0" encoding="utf-8" ?>'
        body += '<D:propfind xmlns:D="DAV:">'
        body += '<D:allprop/></D:propfind>'
        headers = {'Depth': '1'}
        requestpath = datos['instancia']+'remote.php/webdav'
        intentos = 0
        intenta = True
        while intenta:
                try:
                        response = session.request(verb, requestpath, data=body, headers=headers)
                        free = response.text.split("<d:quota-available-bytes>")[1].split("</d:quota-available-bytes>")[0]
                        print (response.status_code)
                        intenta = False
                except Exception as e:
                        print ("intentos: ",intentos)
                        if intentos > 10:
                                intenta = False
                        else:
                                intentos += 1
        mbfree = float(free)/1000000
        usage = response.text.split("<d:quota-used-bytes>")[1].split('</d:quota-used-bytes>')[0]
        mbusage = float(usage)/1000000
        video360 = response.text.split("<d:href>/remote.php/webdav/Carpeta/</d:href>")[1].split('<d:quota-used-bytes>')[1].split('</d:quota-used-bytes>')[0]
        mbvideo360 = float(video360)/1000000
        respuesta_json["OtrosArchivos"]=mbusage-mbvideo360
        respuesta_json["Carpeta"]=mbvideo360
        respuesta_json["Disponible"]=mbfree

        return json.dumps(respuesta_json)

@app.route('/geturlvideo',methods=['POST'])
def api_geturlvideo():
    if request.headers['Content-Type'] == 'application/json':
        respuesta_json = {}
        datos = json.loads(request.data)
        print (datos)
        S3_ACCESS_KEY = 'key'
        S3_SECRET_KEY = 'secretkey'
        client = boto3.client('s3',aws_access_key_id=S3_ACCESS_KEY,aws_secret_access_key=S3_SECRET_KEY)
        if datos["instancia"] == 'AMAZON':
                respuesta = client.generate_presigned_url('get_object',Params={'Bucket':'videoguardado','Key':datos["fileid"]},ExpiresIn=3600)
                return respuesta
        else:
                session = requests.Session()
                session.auth = (datos['token_correo'],datos['token_contrasena'])
                verb = 'POST'
                body = dict(fileId=datos["fileid"])
                headers = {"OCS-APIRequest":"true"}
                requestpath = datos['instancia']+'ocs/v2.php/apps/dav/api/v1/direct'
                response = session.request(verb, requestpath, data=body, headers=headers)
                url = response.text.split("<url>")[1].split("</url>")[0]
                return url
@app.route('/deletevideo',methods=['POST'])
def api_deletevideo():
    if request.headers['Content-Type'] == 'application/json':
        respuesta_json = {}
        datos = json.loads(request.data)
        print (datos)
        S3_ACCESS_KEY = 'key'
        S3_SECRET_KEY = 'secretkey'
        client = boto3.client('s3',aws_access_key_id=S3_ACCESS_KEY,aws_secret_access_key=S3_SECRET_KEY)
        id_camara = datos["id_camara"]
        fecha = datos["fecha"]
        hora = datos["hora"]
        cursor.execute('select json_horas from admin.guardado where id_camara=\'%s\' AND fecha=\'%s\''%(id_camara,fecha))
        json_horas = cursor.fetchone()
        if json_horas:
		print (json_horas)
                if hora in json_horas[0]:
                        id_file = json_horas[0][hora]
                        del json_horas[0][hora]
			json_envio = json.dumps(json_horas[0]).replace("\'","\"")
			if len(json_horas[0]) == 0:
			       cursor.execute('delete from admin.guardado where id_camara=\'%s\' AND fecha=\'%s\''%(id_camara,fecha))
			else:
                               cursor.execute('update admin.guardado set json_horas=\'%s\' where id_camara=\'%s\' AND fecha=\'%s\''%(json_envio,id_camara,fecha))
                        if datos["instancia"] == 'AMAZON':
                               client.delete_object(Bucket='videoguardado',Key=id_file)
                               return "Exitoso"
                        else:
                               session = requests.Session()
                               session.auth = (datos['token_correo'],datos['token_contrasena'])
                               verb = 'POST'
                               body = dict(fileId=datos["fileid"])
                               return "En Construccion"


if __name__ == '__main__':
    app.run(host='0.0.0.0')
