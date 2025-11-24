import mysql.connector
from mysql.connector import Error
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.nodes = {
            'node1': {'host': 'mysql-node1', 'port': 3306, 'db': 'imdb_distributed'},
            'node2': {'host': 'mysql-node2', 'port': 3306, 'db': 'imdb_distributed'},
            'node3': {'host': 'mysql-node3', 'port': 3306, 'db': 'imdb_distributed'}
        }
        self.user = 'root'
        self.password = 'password123'
    
    def get_connection(self, node_name, isolation_level='READ-COMMITTED'):
        """Get database connection with specified isolation level"""
        try:
            node = self.nodes[node_name]
            conn = mysql.connector.connect(
                host=node['host'],
                port=node['port'],
                database=node['db'],
                user=self.user,
                password=self.password
            )
            
            # Set isolation level
            cursor = conn.cursor()
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            cursor.close()
            
            return conn
        except Error as e:
            logger.error(f"Error connecting to {node_name}: {e}")
            return None
    
    def check_all_nodes(self):
        """Check health of all nodes"""
        status = {}
        for node_name in self.nodes.keys():
            conn = self.get_connection(node_name)
            if conn:
                status[node_name] = {'status': 'online', 'healthy': True}
                conn.close()
            else:
                status[node_name] = {'status': 'offline', 'healthy': False}
        return status
    
    def get_titles(self, page=1, limit=20, title_type=None):
        """Get titles with pagination"""
        offset = (page - 1) * limit
        conn = self.get_connection('node1')
        
        if not conn:
            return {'error': 'Node 1 unavailable', 'data': []}
        
        try:
            cursor = conn.cursor(dictionary=True)
            
            if title_type:
                query = """
                    SELECT * FROM titles 
                    WHERE title_type = %s 
                    ORDER BY start_year DESC 
                    LIMIT %s OFFSET %s
                """
                cursor.execute(query, (title_type, limit, offset))
            else:
                query = """
                    SELECT * FROM titles 
                    ORDER BY start_year DESC 
                    LIMIT %s OFFSET %s
                """
                cursor.execute(query, (limit, offset))
            
            titles = cursor.fetchall()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) as total FROM titles" + 
                         (" WHERE title_type = %s" if title_type else ""),
                         (title_type,) if title_type else ())
            total = cursor.fetchone()['total']
            
            return {
                'data': titles,
                'total': total,
                'page': page,
                'limit': limit
            }
        except Error as e:
            logger.error(f"Error fetching titles: {e}")
            return {'error': str(e), 'data': []}
        finally:
            conn.close()
    
    def get_title_by_id(self, tconst):
        """Get single title by ID"""
        conn = self.get_connection('node1')
        
        if not conn:
            return {'error': 'Node 1 unavailable'}
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
            title = cursor.fetchone()
            return title if title else {'error': 'Title not found'}
        except Error as e:
            logger.error(f"Error fetching title: {e}")
            return {'error': str(e)}
        finally:
            conn.close()
    
    def execute_query(self, node_name, query, params=None, isolation_level='READ-COMMITTED'):
        """Execute query on specific node"""
        conn = self.get_connection(node_name, isolation_level)
        
        if not conn:
            return {'success': False, 'error': f'{node_name} unavailable'}
        
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
            return {'success': True, 'rows_affected': cursor.rowcount}
        except Error as e:
            conn.rollback()
            logger.error(f"Error executing query on {node_name}: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def get_transaction_logs(self, limit=50):
        """Get recent transaction logs"""
        conn = self.get_connection('node1')
        
        if not conn:
            return {'error': 'Node 1 unavailable', 'logs': []}
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT * FROM transaction_log 
                ORDER BY timestamp DESC 
                LIMIT %s
            """, (limit,))
            logs = cursor.fetchall()
            return {'logs': logs}
        except Error as e:
            logger.error(f"Error fetching logs: {e}")
            return {'error': str(e), 'logs': []}
        finally:
            conn.close()