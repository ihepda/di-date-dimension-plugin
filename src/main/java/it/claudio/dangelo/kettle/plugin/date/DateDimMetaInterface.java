/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.claudio.dangelo.kettle.plugin.date;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 *
 * @author claudio
 */
public class DateDimMetaInterface extends BaseStepMeta implements StepMetaInterface {

    private DatabaseMeta databaseMeta ;
    private String idVarName;
    private String inputVarName;
    private String tableName;
    private Properties conversions;
    private Map<DateDimensionColumn, String> columns;

    @Override
    public void setDefault() {
    	this.databaseMeta = null;
        this.idVarName = "";
        this.inputVarName = "";
        this.tableName = "";
        this.columns = new HashMap<DateDimensionColumn, String>();
        this.conversions = null;
    }

    public Properties getConversions() {
		return conversions;
	}
    
    public void setConversions(Properties converions) {
		this.conversions = converions;
	}
    
    public DatabaseMeta getDatabaseMeta() {
		return databaseMeta;
	}
    
    public void setDatabaseMeta(DatabaseMeta databaseMeta) {
		this.databaseMeta = databaseMeta;
	}

	public String getIdVarName() {
		return idVarName;
	}


	public void setIdVarName(String idVarName) {
		this.idVarName = idVarName;
	}


	public String getInputVarName() {
		return inputVarName;
	}


	public void setInputVarName(String inputVarName) {
		this.inputVarName = inputVarName;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void addColumnName(DateDimensionColumn type, String columnname) {
		if(this.columns == null)
			this.columns = new HashMap<DateDimensionColumn, String>();
		this.columns.put(type, columnname);
	}
	
	public String getColumnName(DateDimensionColumn type) {
		if(this.columns == null)
			this.columns = new HashMap<DateDimensionColumn, String>();
		return this.columns.get(type);
	}

	Map<DateDimensionColumn, String> getColumns() {
		if(this.columns == null)
			this.columns = new HashMap<DateDimensionColumn, String>();
		return new HashMap<DateDimensionColumn, String>(this.columns);
	}
	
	void setColumns(Map<DateDimensionColumn, String> columns) {
		this.columns = columns;
		if(this.columns == null) this.columns = new HashMap<DateDimensionColumn, String>();
		if(!this.columns.containsKey(DateDimensionColumn.ID) ) this.columns.put(DateDimensionColumn.ID, DateDimensionColumn.ID.name());
	}

	
	@Override
	public void loadXML(Node stepNode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleXMLException {
        try {
            String connectionId = XMLHandler.getTagValue(stepNode, "connectionId");
            if(connectionId != null) 
            	this.databaseMeta = DatabaseMeta.findDatabase(databases, connectionId);
            this.idVarName = XMLHandler.getTagValue(stepNode, "idVarName");
            this.inputVarName = XMLHandler.getTagValue(stepNode, "inputVarName");
            this.tableName = XMLHandler.getTagValue(stepNode, "tableName");
            DateDimensionColumn[] values = DateDimensionColumn.values();
            for (DateDimensionColumn type : values) {
        		String columnName = XMLHandler.getTagValue(stepNode, type.name());
        		if(columnName != null && columnName.length() >0)
        			this.addColumnName(type, columnName);
    		}
            String conversionString = XMLHandler.getTagValue(stepNode, "conversion");
            this.popolateConversion(conversionString);
        } catch (Exception e) {
            throw new KettleXMLException("Unable to load step info from XML", e);
        }
    }
	
	public String conversionString() throws KettleException {
		if(this.conversions == null) return "";
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			this.conversions.store(out, null);
		} catch (IOException e) {
			throw new KettleException(e);
		}
		return out.toString();
	}
	
	public void popolateConversion(String string) throws KettleException {
		if(string == null) return;
		string = string.trim();
		if(string.length() == 0) return;
		if(this.conversions == null) this.conversions = new Properties();
		StringReader reader = new StringReader(string);
		this.conversions.clear();
		try {
			this.conversions.load(reader);
		} catch (IOException e) {
			throw new KettleException(e);
		}
		reader.close();
	}
	
	@Override
    public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        try {
        	if(this.databaseMeta != null) {
        		rep.saveDatabaseMetaStepAttribute(id_transformation, id_step, "connectionId", databaseMeta);
        		/*
        		ObjectId objectId = this.databaseMeta.getObjectId();
				rep.saveStepAttribute(id_transformation, id_step, "connectionId", objectId.getId());
                rep.insertStepDatabase(id_transformation, id_step, objectId);
                */
        	}
            rep.saveStepAttribute(id_transformation, id_step, "idVarName", this.idVarName);
            rep.saveStepAttribute(id_transformation, id_step, "inputVarName", this.inputVarName);
            rep.saveStepAttribute(id_transformation, id_step, "tableName", this.tableName);
            DateDimensionColumn[] values = DateDimensionColumn.values();
            for (DateDimensionColumn type : values) {
        		String columnName = this.getColumnName(type);
        		if(columnName != null) {
        			rep.saveStepAttribute(id_transformation, id_step, type.name(), columnName);
        		}
    		}
            String conversionString = this.conversionString();
            if(conversionString != null)
                rep.saveStepAttribute(id_transformation, id_step, "conversion", conversionString);
        } catch (Exception e) {
            throw new KettleException("Unable to save step information to the repository for id_step=" + id_step, e);
        }
    }

	@Override
    public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException {
        try {
//            long id_connection = rep.getStepAttributeInteger(id_step, "connectionId");
//            databaseMeta = DatabaseMeta.findDatabase(databases, new LongObjectId(id_connection));
        	databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "connectionId", databases);
            this.idVarName = rep.getStepAttributeString(id_step, "idVarName");
            this.inputVarName = rep.getStepAttributeString(id_step, "inputVarName");
            this.tableName = rep.getStepAttributeString(id_step, "tableName");
            DateDimensionColumn[] values = DateDimensionColumn.values();
            for (DateDimensionColumn type : values) {
        		String columnName = rep.getStepAttributeString(id_step, type.name());
        		if(columnName != null)
        			this.addColumnName(type, columnName);
    		}
            String conversionString = rep.getStepAttributeString(id_step, "conversion");
            this.popolateConversion(conversionString);
        } catch (Exception e) {
            throw new KettleException("Unexpected error reading step information from the repository", e);
        }

    }

	@Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info) {
        if(this.idVarName == null || this.idVarName.length() == 0)
            remarks.add(new CheckResult(CheckResult.TYPE_RESULT_ERROR,"Illegal argument, id variable name can't be null",stepMeta));
        if(this.databaseMeta == null)
            remarks.add(new CheckResult(CheckResult.TYPE_RESULT_ERROR,"Illegal argument connection",stepMeta));
        if(this.inputVarName == null || this.inputVarName.length() == 0)
            remarks.add(new CheckResult(CheckResult.TYPE_RESULT_ERROR,"Illegal argument, input variable name can't be null",stepMeta));
        if(this.tableName == null || this.tableName.length() == 0)
            remarks.add(new CheckResult(CheckResult.TYPE_RESULT_ERROR,"Illegal argument, table name can't be null",stepMeta));
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans) {
        return new DateDimStepInterface(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new DateDimStepDataInterface();
    }

    @Override
    public String getXML() throws KettleException {
        StringBuffer retval = new StringBuffer();
        if(this.databaseMeta != null) {
        	retval.append(XMLHandler.addTagValue("connectionId", databaseMeta.getName()));
        }
        retval.append(XMLHandler.addTagValue("idVarName", this.idVarName));
        retval.append(XMLHandler.addTagValue("inputVarName", this.inputVarName));
        retval.append(XMLHandler.addTagValue("tableName", this.tableName));
        DateDimensionColumn[] values = DateDimensionColumn.values();
        for (DateDimensionColumn dateDimensionColumn : values) {
			String columnName = this.getColumnName(dateDimensionColumn);
			if(columnName != null)
				retval.append(XMLHandler.addTagValue(dateDimensionColumn.name(), columnName));
		}
        String conversionString = this.conversionString();
        if(conversionString != null)
            retval.append(XMLHandler.addTagValue("conversion", conversionString));
        return retval.toString();
    }

    RowMetaInterface getRowMetaInterface() throws KettleStepException {
        RowMeta rowMeta = new RowMeta();
        rowMeta.addValueMeta(new ValueMeta(this.idVarName, ValueMetaInterface.TYPE_BIGNUMBER,ValueMetaInterface.STORAGE_TYPE_NORMAL));
        return rowMeta;
    }
    
    private void addValueMetaColumnName(RowMeta rowMeta, DateDimensionColumn name, String defaultName, int type, int length, int precision) {
    	String columnName = this.getColumnName(name);
    	if(columnName == null) columnName = defaultName;
    	if(columnName != null)
    		rowMeta.addValueMeta(new ValueMeta(columnName, type, length, precision));
    }
    
    String getDDL() {
    	if(this.databaseMeta == null)
    		return "Select a connection first";
		Database database = new Database(loggingObject, this.databaseMeta);
		String sql = "";
		try {
			database.connect();
            String schemaTable = this.databaseMeta.getQuotedSchemaTableCombination(null, this.tableName);
    		RowMetaInterface rowMeta = getColumnsRowMeta();
            sql = database.getDDL(schemaTable, rowMeta, "ID", true, "ID");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return sql;
    }

	RowMetaInterface getColumnsRowMeta() {
		RowMeta rowMeta = new RowMeta();
		DateDimensionColumn[] values = DateDimensionColumn.values();
		for (DateDimensionColumn dateDimensionColumn : values) {
			int type = 0 , length= 0, precision = 0;
			String defaultName = null;
			switch (dateDimensionColumn) {
			case DAY:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 2;
				precision = 0;
//				defaultName = dateDimensionColumn.name();
				break;
			case DAY_OF_WEEK:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 1;
				precision = 0;
				break;
			case FOUR_MONTHS:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 1;
				precision = 0;
//				defaultName = dateDimensionColumn.name();
				break;
			case FOUR_MONTHS_NAME :
				type = ValueMetaInterface.TYPE_STRING;
				length = 20;
				precision = 0;
				break;
			case HOUR:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 2;
				precision = 0;
				break;
			case MINUTE:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 2;
				precision = 0;
				break;
			case ID:
				type = ValueMetaInterface.TYPE_BIGNUMBER;
				length = 0;
				precision = 0;
				defaultName = dateDimensionColumn.name();
				break;
			case MONTH:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 2;
				precision = 0;
//				defaultName = dateDimensionColumn.name();
				break;
			case SIX_MONTHS:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 2;
				precision = 0;
				break;
			case SIX_MONTHS_NAME :
				type = ValueMetaInterface.TYPE_STRING;
				length = 20;
				precision = 0;
				break;
			case TIMESTAMP:
				type = ValueMetaInterface.TYPE_NUMBER;
				length = 100;
				precision = 0;
				break;
			case TREE_MONTHS:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 2;
				precision = 0;
				break;
			case TREE_MONTHS_NAME :
				type = ValueMetaInterface.TYPE_STRING;
				length = 20;
				precision = 0;
				break;
			case TWO_MONTHS:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 2;
				precision = 0;
				break;
			case TWO_MONTHS_NAME :
				type = ValueMetaInterface.TYPE_STRING;
				length = 20;
				precision = 0;
				break;
			case WEEK:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 2;
				precision = 0;
				break;
			case YEAR:
				type = ValueMetaInterface.TYPE_INTEGER;
				length = 4;
				precision = 0;
//				defaultName = dateDimensionColumn.name();
				break;
			default:
				break;
			}
			this.addValueMetaColumnName(rowMeta, dateDimensionColumn, defaultName, type, length, precision);
		}
		return rowMeta;
	}

    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) throws KettleStepException {
        if (databaseMeta==null || this.idVarName == null || this.idVarName.length() == 0) throw new KettleStepException("Illegal argument, id variable name can't be null");
        inputRowMeta.addRowMeta(getRowMetaInterface());
    }

}
