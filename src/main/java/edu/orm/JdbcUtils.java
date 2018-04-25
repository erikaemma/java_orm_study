package edu.orm;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;

public abstract class JdbcUtils<E> {
	
	private String uri;
	
	private String username;
	
	private String password;
	
	private String uri_params;
	
	private Connection conn;

	public JdbcUtils(String uri, String username, String password, String[] uri_parameters) {
		this.Init(uri, username, password, uri_parameters);
	}
	
	public JdbcUtils(String uri, String username, String password) {
		this.Init(uri, username, password, null);
	}
	
	public void finalize() {
		if(this.conn != null) {
			if(this.conn != null) {
				try {
					this.conn.close();
				} catch (SQLException se) {
					se.printStackTrace();
				}
			}
		}
	}
	
	private void Init(String uri, String username, String password, String[] uri_parameters) {
		String uri_param = null;
		if(uri_parameters != null) {
			StringBuffer uri_params = new StringBuffer();
			for(int i = 0; i < uri_parameters.length; i++) {
				uri_params.append(uri_parameters[i]);
				uri_params.append('&');
			}
			uri_params.deleteCharAt(uri_params.length() - 1);
			uri_param = uri_params.toString();
		} else
			uri_param = "";
		this.uri = uri;
		this.username = username;
		this.password = password;
		this.uri_params = uri_param;
		this.conn = null;
	}

	public boolean Connection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			this.conn = DriverManager.getConnection(this.uri + '?' + this.uri_params, this.username, this.password);
		} catch (Exception e) {
			System.out.println("连接数据库失败！");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private int executeUpdate(String sql, Object[] parameters) {
		PreparedStatement pstmt = null;
		int result = -1;
		if(this.conn == null)
			return result;
		try {
			pstmt = this.conn.prepareStatement(sql);
			for(int i = 0; i < parameters.length; i++)
				pstmt.setObject(i+1, parameters[i]);
			result = pstmt.executeUpdate();
		} catch (SQLException se) {
			System.out.println("更新数据库失败！");
			//se.printStackTrace();
			System.out.println(se.getMessage());
		} finally {
			if(pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException se2) {
					se2.printStackTrace();
				}
			}
		}
		return result;
	}
	
	private String getTableName(Class<E>clazz) {
		boolean existTableAnno = clazz.isAnnotationPresent(Table.class);
		if(!existTableAnno)
			throw new RuntimeException(clazz + "没有Table注解.");
		Table tableAnno = (Table)clazz.getAnnotation(Table.class);
		return tableAnno.name();
	}
	
	private String getInsertSql(String tableName, int length) {
		StringBuilder sql = new StringBuilder();
		sql.append("insert into ").append(tableName).append(" values(");
		for(int i = 0; i < length; i++)
			sql.append("?,");
		sql.deleteCharAt(sql.length() - 1);
		sql.append(")");
		return sql.toString();
	}
	
	private Object[] getSqlParams(E element, Field[] fields) {
		Object[] params = new Object[fields.length];
		for(int i = 0; i < fields.length; i++) {
			fields[i].setAccessible(true);
			try {
				if(fields[i].isAnnotationPresent(Column.class) && fields[i].getAnnotation(Column.class).type().toLowerCase() == "datetime") {
					params[i] = (Object) DtsTool.Date2TimeStamp((String) fields[i].get(element), null);
				} else if (fields[i].isAnnotationPresent(Id.class) && fields[i].getAnnotation(Id.class).type().toLowerCase() == "datetime") {
					params[i] = (Object) DtsTool.Date2TimeStamp((String) fields[i].get(element), null);
				} else
					params[i] = fields[i].get(element);
			} catch (IllegalAccessException e) {
				System.out.println("获取" + element + "的属性值失败.");
				//e.printStackTrace();
				System.out.println(e.getMessage());
			}
		}
		return params;
	}
	
	public int insert(E element) {
		if(element == null)
			throw new IllegalArgumentException("插入的元素为空.");
		Class clazz = element.getClass();
		String tableName = this.getTableName(clazz);
		Field[] fields = clazz.getDeclaredFields();
		if(fields == null || fields.length == 0)
			throw new RuntimeException(element + "没有属性.");
		String sql = this.getInsertSql(tableName, fields.length);
		Object[] params = this.getSqlParams(element, fields);
		return this.executeUpdate(sql, params);
	}
	
	private String getUpdateSqlAndParams(E element, Object[] params) {
		Class clazz = element.getClass();
		String tableName = this.getTableName(clazz);
		Field[] fields = clazz.getDeclaredFields();
		StringBuilder sql = new StringBuilder();
		sql.append("update ").append(tableName).append(" set");
		String idName = "";
		int index = 0;
		for(int i = 0; i < fields.length; i++) {
			fields[i].setAccessible(true);
			if(fields[i].isAnnotationPresent(Id.class)){
				idName = fields[i].getAnnotation(Id.class).name();
				try {
					params[params.length - 1] = fields[i].get(element);
					if(params[params.length - 1] == null)
						throw new RuntimeException(element + "没有Id属性.");
				} catch (IllegalAccessException e) {
					System.out.println("获取" + element + "的属性值失败.");
					//e.printStackTrace();
					System.out.println(e.getMessage());
				}
			}
			boolean isPresent = fields[i].isAnnotationPresent(Column.class);
			if(isPresent) {
				Column col = fields[i].getAnnotation(Column.class);
				String colName = col.name();
				sql.append(" ").append(colName).append(" = ?,");
				try {
					if(fields[i].getAnnotation(Column.class).type().toLowerCase() == "datetime") {
						params[index++] = (Object) DtsTool.Date2TimeStamp((String) fields[i].get(element), null);
					} else
						params[index++] = fields[i].get(element);
				} catch (IllegalAccessException e) {
					System.out.println("获取" + element + "的属性值失败.");
					//e.printStackTrace();
					System.out.println(e.getMessage());
				}
			}
		}
		sql.deleteCharAt(sql.length() - 1);
		sql.append("where ").append(idName).append(" = ?");
		return sql.toString();
	}
	
	public int updateById(E element) {
		if(element == null)
			throw new IllegalArgumentException("插入的元素为空.");
		Class clazz = element.getClass();
		Field[] fields = clazz.getDeclaredFields();
		if(fields == null || fields.length == 0)
			throw new RuntimeException(element + "没有属性.");
		Object[] params = new Object[fields.length];
		String sql = this.getUpdateSqlAndParams(element, params);
		return this.executeUpdate(sql, params);
	}
	
	public int deleteById(E element) {
		if(element == null)
			throw new IllegalArgumentException("删除的元素为空.");
		Class clazz = element.getClass();
		String tableName = this.getTableName(clazz);
		Field[] fields = clazz.getDeclaredFields();
		if(fields == null || fields.length == 0)
			throw new RuntimeException(element + "没有属性.");
		String idName = "";
		int index = 0;
		Object[] idValue = new Object[1];
		for(int i = 0; i < fields.length; i++) {
			fields[i].setAccessible(true);
			if(fields[i].isAnnotationPresent(Id.class)) {
				idName = fields[i].getAnnotation(Id.class).name();
				index = i;
				break;
			}
		}
		StringBuilder sql = new StringBuilder();
		sql.append("delete from ").append(tableName).append(" where ").append(idName).append(" = ?");
		try {
			if(idName == "")
				throw new RuntimeException(element + "没有Id属性.");
			if (fields[index].getAnnotation(Id.class).type() == "datetime") {
				idValue[0] = (Object) DtsTool.Date2TimeStamp((String) fields[index].get(element), null);
			} else
				idValue[0] = fields[index].get(element);
		} catch (IllegalAccessException e) {
			System.out.println("获取" + element + "的Id属性值失败.");
			//e.printStackTrace();
			System.out.println(e.getMessage());
		}
		return this.executeUpdate(sql.toString(), idValue);
	}
	
//	private ResultSet executeQuery(String sql, Object[] parameters) {
//		PreparedStatement pstmt = null;
//		ResultSet result = null;
//		if(this.conn == null)
//			return null;
//		try {
//			pstmt = this.conn.prepareStatement(sql);
//			for(int i = 0; i < parameters.length; i++)
//				pstmt.setObject(i+1, parameters[i]);
//			result = pstmt.executeQuery();
//		} catch (SQLException se) {
//			System.out.println("查询数据库失败！");
//			//se.printStackTrace();
//			System.out.println(se.getMessage());
//		} finally {
//			if(pstmt != null) {
//				try {
//					pstmt.close();
//				} catch (SQLException se2) {
//					se2.printStackTrace();
//				}
//			}
//		}
//		return result;
//	}
	
	private String firstToUpper(String str) {
		char[] ch = str.toCharArray();
		if(ch[0] >= 'a' && ch[0] <= 'z') {
			ch[0] = (char)(ch[0] - 32);
		}
		return new String(ch);
	}
	
	public Class<E> getType() {
		Type superClass = this.getClass().getGenericSuperclass();
		ParameterizedType parameterizedType = (ParameterizedType)superClass;
		Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
		return (Class) (actualTypeArguments[0]);
	} 
	
//	public List<E> selectById(E element) {
	public Object selectById(Object id) {
		PreparedStatement pstmt = null;
		if(this.conn == null)
			return null;
		if(id == null || (String)id== "")
			throw new IllegalArgumentException("查找的Id为空.");
		Class<E> clazz = (Class<E>) this.getType();
		String className = clazz.getName();
		E obj = null;
		try {
			obj = clazz.newInstance();
//			Class c = null;
//			try {
//				c = Class.forName(className);
//			} catch (ClassNotFoundException e) {
//				System.out.println("没有找到类" + className);
//				e.printStackTrace();
//			}
			String tableName = this.getTableName(clazz);
			Field[] fields = clazz.getDeclaredFields();
			if(fields == null || fields.length == 0)
				throw new RuntimeException(className + "没有属性.");
			String idName = "";
			Object[] idValue = new Object[]{id};
			for(int i = 0; i < fields.length; i++) {
				fields[i].setAccessible(true);
				if(fields[i].isAnnotationPresent(Id.class)) {
					idName = fields[i].getAnnotation(Id.class).name();
					break;
				}
			}
			StringBuilder sql = new StringBuilder();
			sql.append("select * from ").append(tableName).append(" where ");
			if(idName == "")
				throw new RuntimeException(className + "没有Id属性.");
			sql.append(idName).append(" = ?");
			ResultSet rs = null;
			try {
				pstmt = this.conn.prepareStatement(sql.toString());
				pstmt.setObject(1, idValue[0]);
				rs = pstmt.executeQuery();
				//List<E> ret = new ArrayList<E>();
				while (rs.next()) {
					for(int i = 0; i < fields.length; i++) {
						fields[i].setAccessible(true);
						if(!fields[i].isAnnotationPresent(Id.class) && !fields[i].isAnnotationPresent(Column.class))
							continue;
						String fieldType = fields[i].getType().toString().toLowerCase(); //字段类型
						//System.out.println(fieldType);
						//class java.lang.string
						String columnType = ""; //列类型
						String columnName = ""; //列名
						StringBuilder methodName = new StringBuilder(); //字段set名
						if(fields[i].isAnnotationPresent(Id.class)) {
							columnType = fields[i].getAnnotation(Id.class).type().toLowerCase();
							columnName = fields[i].getAnnotation(Id.class).name();
						} else {
							columnType = fields[i].getAnnotation(Column.class).type().toLowerCase();
							columnName = fields[i].getAnnotation(Column.class).name();
						}
						methodName.append("set").append(this.firstToUpper(fields[i].getName()));
						try {
							Method method = obj.getClass().getDeclaredMethod(methodName.toString(), fields[i].getType());
							if(columnType == "datetime")
								method.invoke(obj, DtsTool.getDatetime(rs, columnName));
							switch(fieldType) {
							case "class java.lang.string":
								method.invoke(obj, rs.getString(columnName));
								break;
							case "int":
							case "class java.lang.Integer":
								method.invoke(obj, rs.getInt(columnName));
								break;
							case "double":
							case "class java.lang.Double":
								method.invoke(obj, rs.getDouble(columnName));
								break;
							default:
								System.out.println("不支持的类型");
								break;
							}
						} catch (NoSuchMethodException e) {
							System.out.println("没有找到方法" + methodName.toString());
							e.printStackTrace();
						}
					} //end for
				} //end while
				return obj;
			} catch (SQLException se) {
				se.printStackTrace();
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				if(rs != null) {
					try {
						rs.close();
					} catch(SQLException e) {
						e.printStackTrace();
					}
				}
				if(pstmt != null) {
					try {
						pstmt.close();
					} catch (SQLException se2) {
						se2.printStackTrace();
					}
				}
			}
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		return obj;
	}
}
