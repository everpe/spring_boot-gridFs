package com.example.mongo.binary.api.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

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
		DBObject metadata=new BasicDBObject();
		metadata.put("type", extension);
		metadata.put("user", "ever");
		String archivo=file+"."+extension;
		//Store  File		//el nombre es el tercer Parametro
		//Se puede borrar por el nombre talcual ya q se guarda igual en el filename del documento.
		this.deleteIfExist(file);

		String fileId=gridFsOperations.store(new FileInputStream("C:/Users/Ever/Desktop/Varios/images/"+archivo), 
					   file,"type/"+extension,metadata).getId().toString();
		
		//System.out.println("El id de almacenado fué:  "+fileId);			   
		return "File Stored Sucessfully: "+file +"with FileId : "+fileId;
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
	
	
	
	
	
	///---------------------------------------------------------------------------------------------------------------------
	
	/*@GetMapping("/saveFiles/{parametro}/")Codigo Inicial
	public String saveFile(@PathVariable("parametro") String parametro) throws FileNotFoundException {
		
		 System.out.println("el parametro fué:"+parametro); 
		//Define Metadata
		DBObject metadata=new BasicDBObject();
		metadata.put("organization", "Java Techie");
		
		//Store image File
		InputStream inputstream= new FileInputStream("C:/Users/Ever/Desktop/Varios/images/"+parametro);
		//InputStream inputstream= new FileInputStream("C:/Users/Ever/Desktop/Varios/images/logo.png");
		metadata.put("type", "image");
	
		fileId=gridFsOperations.store(inputstream,"logo.png","image/pgn",metadata).getId().toString();
		System.out.println("File Id Stored"+fileId);
		
		//Store Text File
		metadata.put("type", "data");
		gridFsOperations.store(new FileInputStream("C:/Users/Ever/Desktop/Varios/images/textoPrueba.txt"), "myText.txt","text/plain",metadata);
		
		return "File Stored Sucessfully";
	}
	
	/**
	 * Recupera un recurso de tipo imagen por su idFile y lo guarda en una carpeta preestablecida.
	 * @param id, el idFile con que se almacena en la coleccion Files de GridFS.
	 * @return,mensaje con el nombre del archivo que recuperó de mongo.
	 * @throws IOException
	 *
	@GetMapping("/retrive/image/{id}")
	public String retriveImageFile(@PathVariable("id") String id) throws IOException {
		//String id="5f08744cfc3f9f2540d8aade";
		
		System.out.println("El id recibido fué:"+id);
		
		//fileId
		GridFSDBFile dbFile= gridFsOperations.findOne(new Query(Criteria.where("_id").is(id)));
		
		dbFile.writeTo("C:/Users/Ever/Documents/archivos-gridFS/img.png");
		System.out.println("File Name :"+dbFile.getFilename());
		
		return "Image File Retrived con nombre:"+dbFile.getFilename();
	}
		
	/**
	 * Recupera un recurso de tipo txt y lo guarda en una carpeta. 
	 * @return, el nombre del archivo guardado
	 * @throws IOException
	 *
	@GetMapping("/retrive/text")
	public String retriveTextFile() throws IOException {
		GridFSDBFile dbFile = gridFsOperations.findOne(new Query(Criteria.where("metadata.type").is("data")));
		dbFile.writeTo("C:/Users/Ever/Documents/archivos-gridFS/myTextico.txt");
		System.out.println("File name : " + dbFile.getFilename());
		return "Text File retrived with name : " + dbFile.getFilename() ;
	}
	*/

	
	
}
