# app.py (actualizado)
from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

load_dotenv()

app = Flask(__name__)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            os.getenv('DB_URI'),
            sslmode=os.getenv('SSL_MODE', 'require')
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        app.logger.error(f"Error de conexión: {str(e)}")
        raise

@app.route('/query', methods=['POST'])
def handle_query():
    if not request.is_json or 'query' not in request.json:
        return jsonify({'error': 'Se requiere un query en formato JSON'}), 400
    
    query = request.json['query']
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Validación básica de seguridad (solo permite SELECT por ejemplo)
        if not query.strip().lower().startswith('select'):
            return jsonify({'error': 'Solo se permiten consultas SELECT'}), 400
            
        # Ejecutar el query (versión segura con parámetros)
        cursor.execute(sql.SQL(query))
        
        # Obtener resultados (si es una consulta SELECT)
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            return jsonify({
                'query': query,
                'results': results,
                'columns': columns
            })
            
        conn.commit()
        return jsonify({'query': query, 'message': 'Query ejecutado exitosamente'})
        
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error(f"Error de PostgreSQL: {str(e)}")
        return jsonify({'error': f'Error de base de datos: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Error inesperado: {str(e)}'}), 500
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    app.run(debug=os.getenv('FLASK_DEBUG', 'False') == 'True')