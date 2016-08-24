package corp.cencosud.roble;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400JDBCDriver;


public class InicioPrograma {

	private static BufferedWriter bw;
	private static String path;

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private static void crearTxt(Map <String, String> mapArguments) {

		Connection dbconnection = crearConexion();
		Connection dbconnDB2 = crearConexionDB2();
		File file1              = null;
		File file2              = null;
		BufferedWriter bw       = null;
		BufferedWriter bw2      = null;
		PreparedStatement pstmt = null;
		StringBuffer sb         = null;
		String cantMensajes     = null;
		int iFechaIni           = 0;
		int iFechaFin           = 0;

		try {

			try {

				iFechaIni = restarDia(mapArguments.get("-fi"));
				iFechaFin = restarDia(mapArguments.get("-ff"));

			}
			catch (Exception e) {

				e.printStackTrace();
			}
			
			file1                   = new File(path + "/HUB-" + iFechaIni + ".txt");
			file2                   = new File(path + "/HUB-Completo-" + iFechaIni + ".txt");
			
			sb = new StringBuffer();
			sb.append("select ");
			sb.append("T.ID as ID_TRX, ");
			sb.append("(CASE when T.ID_FOP = 1 THEN 'TBK_KCC' WHEN T.ID_FOP = 5 THEN 'CAT2' WHEN T.ID_FOP = 7 THEN 'Visa/MC Cencosud' ");
			sb.append("WHEN T.ID_FOP = 13 THEN 'Cat-RCI' WHEN  T.ID_FOP = 11 THEN 'TBK_SOAP' ELSE VARCHAR(T.ID_FOP) END) AS Medio_Pago, ");
			sb.append("T.ORDER_ID as OC, ");
			sb.append("BF2.AMOUNT as Monto, ");
			sb.append("T.CREATED as Fecha_creacion, ");
			sb.append("BF2.AUTH_DATE as Fecha_autorizacion, ");
			sb.append("T.LAST_UPDATE as Fecha_actualizacion, ");
			sb.append("BF2.CREDIT_CARD_NUMBER as Tarjeta_de_credito, ");
			sb.append("BF2.ID as ID_CARRO_COMPRA, ");
			sb.append("(CASE when T.ID_STATUS = -2 THEN 'TRX_WITHOUT_PAYMENT' WHEN  T.ID_STATUS = -1 THEN 'TRX_WAITING_FOR_REPLY' WHEN  T.ID_STATUS = 0 THEN 'TRX_REJECTED'  WHEN  T.ID_STATUS = 1 THEN 'TRX_APPROVED' ");
			sb.append("ELSE VARCHAR(T.ID_STATUS) END) AS ESTADO_DE_LA_TRX, ");
			sb.append("BF2.ID_STATUS as TBF_STATUS, ");
			sb.append("bf2.AUTHORIZATION_CODE as Codigo_autorizacion , ");
			sb.append("T.SETTLED_AMOUNT as settled_amount, ");
			sb.append("T.ID_APPLICATION as id_application, ");
			sb.append("T.QUERIED_AFTER_RESPONSE as queried_after_response, ");
			sb.append("BF2.REJECT as reject, ");
			sb.append("count(M.ID) as Cant_Messages ");
			sb.append("from phubusr.TRANSACTION T, ");
			sb.append("phubusr.TRANSACTION_BY_FOP BF2, ");
			sb.append("phubusr.MESSAGE M ");
			sb.append("where ");
			sb.append("t.ID_APPLICATION in (1001, 1002) ");
			sb.append("and cast(T.CREATED as date) = DATE(CURRENT TIMESTAMP - 1 DAY) ");
			sb.append("and (t.ID_STATUS = 1 or t.ID_STATUS = -1) ");
			sb.append("and BF2.id_status =1 ");
			sb.append("and T.ID = BF2.ID_TRANSACTION ");
			sb.append("and BF2.ID = M.ID_TRANSACTION_BY_FOP ");
			sb.append("group by ");
			sb.append("T.ID, ");
			sb.append("T.ID_FOP, ");
			sb.append("T.ORDER_ID, ");
			sb.append("BF2.AMOUNT, ");
			sb.append("T.CREATED, ");
			sb.append("BF2.AUTH_DATE, ");
			sb.append("T.LAST_UPDATE, ");
			sb.append("BF2.CREDIT_CARD_NUMBER, ");
			sb.append("BF2.ID, ");
			sb.append("T.ID_STATUS, ");
			sb.append("BF2.ID_STATUS, ");
			sb.append("bf2.AUTHORIZATION_CODE, ");
			sb.append("T.SETTLED_AMOUNT, ");
			sb.append("T.ID_APPLICATION, ");
			sb.append("T.QUERIED_AFTER_RESPONSE,  ");
			sb.append("BF2.REJECT ");
			sb.append("order by T.CREATED ");


			pstmt        = dbconnDB2.prepareStatement(sb.toString());
			sb           = new StringBuffer();
			ResultSet rs = pstmt.executeQuery();
			bw           = new BufferedWriter(new FileWriter(file1));
 
			bw.write("ID_TRX;");
			bw.write("Medio Pago;");
			bw.write("OC;");
			bw.write("Monto;");
			bw.write("Fecha creacion;");
			bw.write("Fecha autorizacion;");
			bw.write("Fecha actualizacion;");
			bw.write("Tarjeta de credito;");
			bw.write("ID_CARRO_COMPRA;");
			bw.write("ESTADO DE LA TRX;");
			bw.write("TBF_STATUS;");
			bw.write("Código autorizacion;");
			bw.write("T.SETTLED_AMOUNT;");
			bw.write("T.ID_APPLICATION;");
			bw.write("T.QUERIED_AFTER_RESPONSE;");
			bw.write("BF2.REJECT;");
			bw.write("Cant. Messages\n");
			
			bw2           = new BufferedWriter(new FileWriter(file2));
			bw2.write("ID_TRX;");
			bw2.write("Medio Pago;");
			bw2.write("OC;");
			bw2.write("Monto;");
			bw2.write("Fecha creacion;");
			bw2.write("Fecha autorizacion;");
			bw2.write("Fecha actualizacion;");
			bw2.write("Tarjeta de credito;");
			bw2.write("ID_CARRO_COMPRA;");
			bw2.write("ESTADO DE LA TRX;");
			bw2.write("TBF_STATUS;");
			bw2.write("Código autorizacion;");
			bw2.write("T.SETTLED_AMOUNT;");
			bw2.write("T.ID_APPLICATION;");
			bw2.write("T.QUERIED_AFTER_RESPONSE;");
			bw2.write("BF2.REJECT;");
			bw2.write("Cant. Messages\n");

			while (rs.next()) {
				
				cantMensajes = rs.getString("Cant_Messages");
				if(cantMensajes != null){
					
					bw2.write(rs.getString("ID_TRX") + ";");
					bw2.write(rs.getString("Medio_Pago") + ";");
					bw2.write(rs.getString("OC") + ";");
					bw2.write(rs.getString("Monto") + ";");
					bw2.write(rs.getString("Fecha_creacion") + ";");
					bw2.write(rs.getString("Fecha_autorizacion") + ";");
					bw2.write(rs.getString("Fecha_actualizacion") + ";");
					bw2.write(rs.getString("Tarjeta_de_credito") + ";");
					bw2.write(rs.getString("ID_CARRO_COMPRA") + ";");
					bw2.write(rs.getString("ESTADO_DE_LA_TRX") + ";");
					bw2.write(rs.getString("TBF_STATUS") + ";");
					bw2.write(rs.getString("Codigo_autorizacion") + ";");
					bw2.write(rs.getString("settled_amount") + ";");
					bw2.write(rs.getString("id_application") + ";");
					bw2.write(rs.getString("queried_after_response") + ";");
					bw2.write(rs.getString("reject") + ";");
					bw2.write(rs.getString("Cant_Messages") + "\n");

					if(!ejecutarQuery2(limpiarCeros(rs.getString("OC")), dbconnection) &&  Integer.parseInt(cantMensajes) >= 4){
						bw.write(rs.getString("ID_TRX") + ";");
						bw.write(rs.getString("Medio_Pago") + ";");
						bw.write(rs.getString("OC") + ";");
						bw.write(rs.getString("Monto") + ";");
						bw.write(rs.getString("Fecha_creacion") + ";");
						bw.write(rs.getString("Fecha_autorizacion") + ";");
						bw.write(rs.getString("Fecha_actualizacion") + ";");
						bw.write(rs.getString("Tarjeta_de_credito") + ";");
						bw.write(rs.getString("ID_CARRO_COMPRA") + ";");
						bw.write(rs.getString("ESTADO_DE_LA_TRX") + ";");
						bw.write(rs.getString("TBF_STATUS") + ";");
						bw.write(rs.getString("Codigo_autorizacion") + ";");
						bw.write(rs.getString("settled_amount") + ";");
						bw.write(rs.getString("id_application") + ";");
						bw.write(rs.getString("queried_after_response") + ";");
						bw.write(rs.getString("reject") + ";");
						bw.write(rs.getString("Cant_Messages") + "\n");
					}

				}
			}

			info("Archivos creados.");
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnection,pstmt,bw);
			cerrarTodo(dbconnDB2, null, bw2);
		}
	}

	private static boolean ejecutarQuery2(String oc, Connection dbconnection) {

		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		boolean existe          = true;
		
		try {
			int numeroOC = Integer.parseInt(oc);
			
			sb = new StringBuffer();
			sb.append("select ");
			sb.append("cast(a.NUMORDEN as char(20)) as Numero_Orden ");
			sb.append("from CECEBUGD.SVVIF03 a ");
			sb.append("where a.NUMORDEN = ? ");

			pstmt = dbconnection.prepareStatement(sb.toString());
			pstmt.setInt(1, numeroOC);
			ResultSet rs = pstmt.executeQuery();
			sb = new StringBuffer();

			if(!rs.next()){
				existe = false;
			}
			
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[crearTxt2]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(null,pstmt,null);
		}
		return existe;
	}

	private static Connection crearConexion() {

		System.out.println("Creando conexion a ROBLE.");
		AS400JDBCDriver d = new AS400JDBCDriver();
		String mySchema = "RDBPARIS2";
		Properties p = new Properties();
		AS400 o = new AS400("roble.cencosud.corp","USRCOM", "USRCOM");
		Connection dbconnection = null;

		try {

			System.out.println("AuthenticationScheme: "+o.getVersion());
			dbconnection = d.connect (o, p, mySchema);
			System.out.println("Conexion a ROBLE CREADA.");
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
		}
		return dbconnection;
	}

	private static Connection crearConexionDB2() {
		
		System.out.println("Creando conexion a HUB.");
		Connection dbconnection = null;

		try {

			Class.forName("com.ibm.db2.jcc.DB2Driver");
			dbconnection = DriverManager.getConnection("jdbc:db2://spp36db04r:50051/PHUBP01","con_hubp","82ndy78hdjos");
			System.out.println("Conexion a HUB CREADA.");
		}
		catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}

	private static String limpiarCeros(String str) {

		int iCont = 0;

		while (str.charAt(iCont) == '0') {

			iCont++;
		}
		return str.substring(iCont, str.length());
	}

	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}

	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {

			System.out.println("Exception:"+e.getMessage());
		}
	}

	private static int restarDia(String sDia) {

		int dia = 0;
		String sFormato = "yyyyMMdd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormato);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -1);
			sDiaAux = df.format(diaAux.getTime());
			dia = Integer.parseInt(sDiaAux);
		}
		catch (Exception e) {

			info("[restarDia]Exception:"+e.getMessage());
		}
		return dia;
	}
}
