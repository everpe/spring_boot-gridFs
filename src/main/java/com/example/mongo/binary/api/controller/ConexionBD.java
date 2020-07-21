package com.example.mongo.binary.api.controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import com.sun.rowset.CachedRowSetImpl;


public class ConexionBD {
	
	public  String usuario; 
	public  String clave; 
	public  String ip; 
	public  String puerto;
	public  String basedatos;
	public  Connection connection;	
	public  List<PreparedStatement> pstms;


	public Connection conectar() throws ClassNotFoundException, SQLException{			
		Class.forName("org.postgresql.Driver");		
		Connection conn =  DriverManager.getConnection("jdbc:postgresql://"+ip+":"+puerto+"/"+basedatos+"", usuario,clave);				
		return conn;		
	}
	
	public  boolean desconectar(Connection con) {		
		try{
			if(con == null)
				return false;
			
			con.close();		
			return true;
		}catch(Exception er){}		
		return false;
	}
	
	public CachedRowSet consultar(String sql,  Object... params) throws SQLException{		
		if(connection == null)
			throw new SQLException("La consulta no se encuentra en transaccion.");
					
		try{
			PreparedStatement s = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
			for(int i = 0;i < params.length; i++)
				s.setObject(i + 1, params[i]);
			
			ResultSet rs = s.executeQuery();
			CachedRowSet crs = new CachedRowSetImpl();
			crs.populate(rs);
			rs.close();
			s.close();//
			
			return crs;				
		}catch(SQLException sex){			
			throw new SQLException(sex.getMessage()); 
		}
	}
		
	public String ejecutar(String sql, Object... params) throws SQLException {		
		if(pstms == null || connection == null)
			throw new SQLException("La consulta no se encuentra en transaccion.");
		
		String id="";
		PreparedStatement pstm = connection.prepareStatement(sql,PreparedStatement.RETURN_GENERATED_KEYS);		
		for(int i = 0;i < params.length; i++)
			pstm.setObject(i + 1, params[i]);
		
		pstm.executeUpdate();		
		ResultSet res = pstm.getGeneratedKeys();
		if(res != null && res.next()){
			id = res.getString(1);
		}
		pstms.add(pstm);
		return id;
	}
	
	public void init() throws ClassNotFoundException, SQLException {		
		connection = conectar();		
		connection.setAutoCommit(false);
		pstms = new ArrayList<PreparedStatement>(); 			
	}
	
	public void finish() throws SQLException {		
		try{
			connection.commit();
			for(PreparedStatement pstm : pstms)
				pstm.close();
			
			pstms = null;			
		}catch (Exception er) {
			connection.rollback();
		}		
		connection.close();
	}
	
	public void close(){		
		if(connection == null)
			return;						
		try{				
			if(connection.isClosed())
				return;
						
			connection.rollback();			
			while( !connection.isClosed() )
				connection.close();
									
		}catch (Exception er) {}		
	}
}
