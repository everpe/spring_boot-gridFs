package com.example.mongo.binary.api.controller;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import javax.sql.rowset.CachedRowSet;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;

/**
 * Realiza la interacción con la BD las dos colecciones de GridFs.
 * @author Ever
 */
@RestController
public class BinaryDataController {
	//Interfaz de operaciones para Grid provista por SpringBoot
	@Autowired
	private GridFsOperations gridFsOperations;
	
	
	/**
	 * Guarda un pdf en GridFS.
	 * @param parametro, el nombre del archivo sin la extensión esa se agrega en el método
	 * @return, mensaje con el fileId del archivo. 
	 * @throws FileNotFoundException, puede que no encuentre el nombre del archivo en el directorio preestablecido.
	 */
	@GetMapping("/savePdf/{file}")
	public String savePdf(@PathVariable("file") String file) throws FileNotFoundException {
		
		String extension=".pdf"; 
		
		//Define Metadata
		DBObject metadata=new BasicDBObject();
		
		metadata.put("type", "data");
		//Store  File		//el nombre es el tercer Parametro
		String fileId=gridFsOperations.store(new FileInputStream("C:/Users/Ever/Desktop/Varios/images/"+file+extension), 
					   file,"type/pdf",metadata).getId().toString();
		
		//System.out.println("El id de almacenado fué:  "+fileId);			   
		return "File Stored Sucessfully: "+file+"with FileId : "+fileId;
	}
	
	
	
	
	/**
	 * Sube un archivo en general desde un directorio especificado a gridFs
	 * @param file, el nombre del archivo
	 * @return mensaje con el nombre del arhivo guardado.
	 * @throws FileNotFoundException,lanza excepción si no puede encontrar  el archivo en el directorio.
	 */
	@GetMapping("/saveFile/{file}/{extension}")
	public String saveFile(@PathVariable("file") String file,
					@PathVariable("extension") String extension) throws FileNotFoundException {
		
		
		//Define Metadata
		/*DBObject metadata=new BasicDBObject();
		metadata.put("type", extension);
		metadata.put("user", "ever");
		String archivo=file+"."+extension;
		//Store  File		//el nombre es el tercer Parametro
		//Se puede borrar por el nombre talcual ya q se guarda igual en el filename del documento.
		//this.deleteIfExist(file);

		String fileId=gridFsOperations.store(new FileInputStream("C:/Users/Ever/Desktop/Varios/images/"+archivo), 
					   file,"type/"+extension,metadata).getId().toString();
		
		//System.out.println("El id de almacenado fué:  "+fileId);			   
		System.out.println( new Date() );*/
		generar();
		return "Todo bien";
	}
	
	
	
	
	/**
	 * Borra el archivo por nombre si ya existe, se puede cambiar a por Id. 
	 * @param name, el nombre del archivo que se va a eliminar.
	 * @return si existia y se borro el archivo true, false si no se encontró el archivo.
	 */
	private boolean deleteIfExist(String name) {	
		GridFSDBFile file=gridFsOperations.findOne(new Query(Criteria.where("filename").is(name)));
		if(file!=null) {
			gridFsOperations.delete(new Query(Criteria.where("filename").is(name)));
			return true;
		}
		return false;
	}
	
	
	@GetMapping("/updateMetadata/{id}")
	public String updateMetadata( @PathVariable("id") String id) {
		GridFSDBFile outputFile = gridFsOperations.findOne(new Query(Criteria.where("_id").is(id)));
	    
		
		BasicDBObject updatedMetadata = new BasicDBObject();
		updatedMetadata.put("type", "data");
		updatedMetadata.put("user", "ever3");;

	    outputFile.setMetaData(updatedMetadata);       
	    outputFile.save();
	    return "metadata Updated Sucessfully";
	}
	
	
	
	
	
	
	/**
	 * Obtiene una imagen con extension png
	 * @param id es el fileId de la coleccion Files(metadatos)
	 * @return nombre del archivo recuperado de la BD.
	 * @throws IOException, lanza excepción si no encuentra el archivo o si no puede escribir el archivo obtenido.
	 */
	@GetMapping("/retriveFile/{id}")
	public String retriveFile(@PathVariable("id") String id) throws IOException {
		//@Note:"LA EXTENSION también LA PODRÍA OBTENER del content/type con un split"
		
		//Busqueda del archivo
		GridFSDBFile dbFile = gridFsOperations.findOne(new Query(Criteria.where("_id").is(id)));
		
		String extension=dbFile.getMetaData().get("type").toString();
		
		//armando el archivo con su extension
		String archivo=dbFile.getFilename()+"."+extension;
		//inserta el archivo encontrado en x carpeta
		dbFile.writeTo("C:/Users/Ever/Documents/archivos-gridFS/"+archivo);
		
		System.out.println("Name File Obtained was: " + dbFile.getFilename());
		return "Text File retrived with name : " + dbFile.getFilename() ;
	}
	
	  
	/**
	 * Obtiene un archivo por su idField. "toca hacer distintos metodos por la extension o mirar como se va a trabajar" 
	 * @return, el nombre del archivo que obtuvó de la bd.
	 * @throws IOException,lanza excepción si no encuentra el archivo
	 *  o si no puede escribir en el directorio el archivo obtenido.
	 */
	@GetMapping("/retrivePdf/{id}")
	public String retrivePdf(@PathVariable("id") String id) throws IOException {
		
		//Es necesario para cuando reescriba el documento no lo deje plano
		String extension=".pdf";
		
		//Busqueda del archivo
		GridFSDBFile dbFile = gridFsOperations.findOne(new Query(Criteria.where("_id").is(id)));
		//inserta el archivo encontrado en x carpeta
		dbFile.writeTo("C:/Users/Ever/Documents/archivos-gridFS/"+dbFile.getFilename()+extension);
		
		System.out.println("Name File Obtained was: " + dbFile.getFilename());
		
		return "Text File retrived with name : " + dbFile.getFilename() ;
	}
	
	
	
	
	
	
	
	
	
	public void generar() {
		//conexión con postgres
		ConexionBD con = getConnection();
		try{
			//mongo
			DBObject metadata=new BasicDBObject();
			metadata.put("type", "pdf");
			metadata.put("user", "ever");
			System.out.println( "Inicio" );
			System.out.println( new Date() );
			con.init();
			//consulta a postgres
			CachedRowSet crs = con.consultar("select d.data_gen,tpd.nombre_documento as nombre, d.codigo_detalle  from predi.predi_detalle_etapa_documento_notificacion as d  inner join predi.predi_tipo_documento as tpd on tpd .codigo_tipo_documento=d.fk_codigo_documento and d.data_gen notnull limit 700");
			while(crs.next()) {		
				File file = new File("/tmp/".concat( crs.getString("nombre") ).concat( crs.getString("codigo_detalle") ).concat(".pdf") );
				//forma el archivo;
				FileUtils.writeByteArrayToFile(file, crs.getBytes("data_gen"));
				if(!file.exists())
					throw new Exception("No existe el archivo");
				
				FileInputStream fin = new FileInputStream(file);
				String fileId=gridFsOperations.store(fin, 
						   crs.getString("nombre"),"type/"+"pdf",metadata).getId().toString();
				con.ejecutar("INSERT INTO predi.data_mongo (id_mongo, id_doc) VALUES(?, ?);", fileId, crs.getString("codigo_detalle"));
				//crs.close();cierra todo
				if(file.exists()) 
					file.delete();					
				fin.close();
			}			
			con.finish();
			System.out.println( "Fin" );
			System.out.println( new Date() );
		}catch (Exception er) {
			er.printStackTrace();
			//con.close();
		}
	}

	
	public ConexionBD getConnection() {
		ConexionBD con = new ConexionBD();
		con.basedatos = "san_vicente";
		con.clave= "password";
		con.ip="localhost";
		con.puerto= "5432";
		con.usuario="postgres";
		return con;
	}
	
}
