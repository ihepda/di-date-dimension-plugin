package it.claudio.dangelo.kettle.plugin.date;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 *
 * @author claudio
 */
public class DateDimStepInterface extends BaseStep implements StepInterface {

	private static final Properties DEFAULT_CONVERSION = new Properties();
	static {
		DEFAULT_CONVERSION.put(DateDimensionColumn.FOUR_MONTHS.name()+".1", "FOUR_1");
		DEFAULT_CONVERSION.put(DateDimensionColumn.FOUR_MONTHS.name()+".2", "FOUR_2");
		DEFAULT_CONVERSION.put(DateDimensionColumn.FOUR_MONTHS.name()+".3", "FOUR_3");
		DEFAULT_CONVERSION.put(DateDimensionColumn.SIX_MONTHS.name()+".1", "SIX_1");
		DEFAULT_CONVERSION.put(DateDimensionColumn.SIX_MONTHS.name()+".2", "SIX_2");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TREE_MONTHS.name()+".1", "TREE_1");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TREE_MONTHS.name()+".2", "TREE_2");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TREE_MONTHS.name()+".3", "TREE_3");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TREE_MONTHS.name()+".4", "TREE_4");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TWO_MONTHS.name()+".1", "TWO_1");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TWO_MONTHS.name()+".2", "TWO_2");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TWO_MONTHS.name()+".3", "TWO_3");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TWO_MONTHS.name()+".4", "TWO_4");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TWO_MONTHS.name()+".5", "TWO_5");
		DEFAULT_CONVERSION.put(DateDimensionColumn.TWO_MONTHS.name()+".6", "TWO_6");
	}

    private DateDimMetaInterface emi;
    private DateDimStepDataInterface esdi;
    public DateDimStepInterface(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        emi = (DateDimMetaInterface) stepMeta.getStepMetaInterface();
        esdi = (DateDimStepDataInterface) stepDataInterface;
    }

    private int shiftMonth(int month, int base) {
		int mod =  month % base;
		int val = month/base;
		if(mod >0 ) val++;
		return val;
    }
    
    private Number getId(HashMap<String, Object> values ) throws KettleException {
    	this.logDebug("Execute the query {0}:", esdi.sqlLoad);
    	this.logDebug("Query data {0}:", values);
    	Number id = null;
    	ResultSet resultSet = null;
    	PreparedStatement prepStmLoad =null;
    	try {
    		int i = 0;
			prepStmLoad = esdi.database.prepareSQL(esdi.sqlLoad);
			List<String> sqlLoadColumns = esdi.sqlLoadColumns;
			for (String columnName : sqlLoadColumns) {
				prepStmLoad.setObject(++i, values.get(columnName));
			}
			resultSet = prepStmLoad.executeQuery();
			if(resultSet.next())
				id = resultSet.getBigDecimal(1);
			return id;
		} catch (Exception e) {
			throw new KettleException(e);
		}finally {
			if(resultSet != null) try {resultSet.close();} catch (SQLException sqlException) {this.logError("<WARN> Error to close result set", sqlException);}
			if(prepStmLoad != null) try {prepStmLoad.close();} catch (SQLException sqlException) {this.logError("<WARN> Error to close prepared statement", sqlException);}
		}
    }
    
    private void insert(HashMap<String, Object> values ) throws KettleException {
    	this.logDebug("Execute insert {0} \n with data {1} ", esdi.sqlInsert, values);
    	PreparedStatement statement = null;
    	try {
			statement = esdi.database.prepareSQL(esdi.sqlInsert);
			int i = 0;
			List<String> sqlInsertColumns = esdi.sqlInsertColumns;
			for (String columnName : sqlInsertColumns) {
				statement.setObject(++i, values.get(columnName));	
			}
			int result = statement.executeUpdate();
			if(result != 1) 
				throw new SQLException("Invalid result of the insert : " + result);
			if(!esdi.database.isAutoCommit())
				esdi.database.commit();
		} catch (Exception e) {
			if(!esdi.database.isAutoCommit())
				esdi.database.rollback();
			throw new KettleException(e);
		}finally {
			if(statement != null) try {statement.close();} catch (SQLException sqlException) {this.logError("<WARN> Error to close prepared statement", sqlException);}
		}
    }
    
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    	Object[] row = this.getRow();
    	if(row == null) {
    		this.setOutputDone();
    		return false;
    	}
    	incrementLinesInput();
    	RowMetaInterface inputRowMeta = this.getInputRowMeta();
		Date date = inputRowMeta.getDate(row, emi.getInputVarName(), null);
    	if(date == null) {
    		this.addRow(inputRowMeta, row, null);
//            this.setOutputDone();
            return true;
    	}
    	GregorianCalendar calendar = new GregorianCalendar();
    	calendar.setTime(date);
    	Properties conversion = new Properties();
    	conversion.putAll(DEFAULT_CONVERSION);
    	if(emi.getConversions() != null) conversion.putAll(emi.getConversions());
    	HashMap<String, Object> values = new HashMap<String, Object>();
    	Map<DateDimensionColumn, String> columnNamesForSQL = this.esdi.columnNamesForSQL;
    	Set<DateDimensionColumn> columnsKey = columnNamesForSQL.keySet();
    	for (DateDimensionColumn dateDimensionColumn : columnsKey) {
			String columnName = esdi.columnNamesForSQL.get(dateDimensionColumn);
			Object value = null;
			int month = calendar.get(Calendar.MONTH)+1;
			switch (dateDimensionColumn) {
			case DAY:
				value = calendar.get(Calendar.DAY_OF_MONTH);
				break;
			case DAY_OF_WEEK:
				int dow = calendar.get(Calendar.DAY_OF_WEEK);
				dow--;
				if(dow == 0) dow = 7;
				value = dow;
				break;
			case FOUR_MONTHS:
				value = this.shiftMonth(month, 4);
				break;
			case FOUR_MONTHS_NAME:
				value = conversion.get(DateDimensionColumn.FOUR_MONTHS+"."+this.shiftMonth(month, 4));
				break;
			case HOUR:
				value = calendar.get(Calendar.HOUR_OF_DAY);
				break;
			case ID:
				continue;
			case MINUTE:
				value = calendar.get(Calendar.MINUTE);
				break;
			case MONTH:
				value = month;
				break;
			case SIX_MONTHS:
				value = this.shiftMonth(month, 6);
				break;
			case SIX_MONTHS_NAME:
				value = conversion.get(DateDimensionColumn.SIX_MONTHS+"."+this.shiftMonth(month, 6));
				break;
			case TIMESTAMP:
				value = date.getTime();
				break;
			case TREE_MONTHS:
				value = this.shiftMonth(month, 3);
				break;
			case TREE_MONTHS_NAME:
				value = conversion.get(DateDimensionColumn.TREE_MONTHS+"."+this.shiftMonth(month, 3));
				break;
			case TWO_MONTHS:
				value = this.shiftMonth(month, 2);
				break;
			case TWO_MONTHS_NAME:
				value = conversion.get(DateDimensionColumn.TWO_MONTHS+"."+this.shiftMonth(month, 2));
				break;
			case WEEK:
				value = calendar.get(Calendar.WEEK_OF_YEAR);
				break;
			case YEAR:
				value = calendar.get(Calendar.YEAR);
				break;
			default:
				break;
			}
			values.put(columnName, value);
		}
    	Number id = this.getId(values);
    	if(id == null)  {
    		this.insert(values);
    		id = this.getId(values);
    	}
    	if(id == null)
    		throw new KettleException("I don't be able to load the dimension  id. The insert don't work?");
    	this.addRow(inputRowMeta, row, id);
    	return true;
    }

	private Object[] addRow(RowMetaInterface rowMetaResult,
			Object[] rowData, Number id) throws KettleStepException {
		// create a new array of datas with the new metadata
		RowMetaInterface outputRowMeta = rowMetaResult.clone();
		outputRowMeta.addRowMeta(emi.getRowMetaInterface());
		int size = outputRowMeta.size();
		Object[] newRow = Arrays.copyOf(rowData, rowData.length);
		newRow = RowDataUtil.resizeArray(newRow, size);
		int index = outputRowMeta.indexOfValue(emi.getIdVarName());
		newRow[index] = id;
		putRow(outputRowMeta, newRow);
		incrementLinesOutput();
		return newRow;
	}

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        super.dispose(smi, sdi);
        DateDimStepDataInterface esdi = (DateDimStepDataInterface) sdi;
        if(esdi.database != null) try{esdi.database.disconnect();}catch(Exception ex) {logError("<WARN> Error to close connection", ex);}
    }

    private Map<DateDimensionColumn, String> getColumnNamesForSQL() {
    	HashMap<DateDimensionColumn, String> result = new HashMap<DateDimensionColumn, String>();
    	Map<DateDimensionColumn, String> columns = emi.getColumns();
    	DateDimensionColumn[] values = DateDimensionColumn.values();
    	for (DateDimensionColumn dateDimensionColumn : values) {
			String columnName = columns.get(dateDimensionColumn);
			if(columnName == null) {
				switch (dateDimensionColumn) {
				case ID:
					columnName = dateDimensionColumn.name();
					break;
				default:
					break;
				}
			}
			if(columnName != null) result.put(dateDimensionColumn, columnName);
		}
    	return result;
    }
    
    private String getSqlLoad(RowMetaInterface rowMetaInterface, Map<DateDimensionColumn, String> columnNames) {
    	List<String> sqlLoadColumns = new ArrayList<String>();
    	StringBuffer buffer = new StringBuffer("SELECT ");
    	List<ValueMetaInterface> valueMetaList = rowMetaInterface.getValueMetaList();
    	int size = valueMetaList.size()-1;
//    	for (int i = 0; i < size ; i++) 
//    		buffer.append(valueMetaList.get(i).getName()).append(", ");
//    	buffer.append(valueMetaList.get(size).getName());
    	String idColumnName = columnNames.get(DateDimensionColumn.ID);
    	buffer.append(idColumnName);
    	buffer.append(" FROM ").append(emi.getTableName());
    	buffer.append(" WHERE ");
    	for (int i = 0; i < size ; i++) {
    		String name = valueMetaList.get(i).getName();
    		sqlLoadColumns.add(name);
    		if(name.equalsIgnoreCase(idColumnName)) continue;
    		if(i > 0)
    			buffer.append(" AND ");
			buffer.append(name).append(" = ? ");
    	}
    	String name = valueMetaList.get(size).getName();
    	if(!name.equalsIgnoreCase(idColumnName))buffer.append(" AND ").append(name).append(" = ? ");
    	esdi.sqlLoadColumns = sqlLoadColumns;
    	return buffer.toString();
    }
    
    private String getSqlInsert(RowMetaInterface rowMetaInterface, Map<DateDimensionColumn, String> columnNames) {
    	List<String> sqlInsertColumns = new ArrayList<String>();
    	String idColumnName = columnNames.get(DateDimensionColumn.ID);
    	List<ValueMetaInterface> valueMetaList = rowMetaInterface.getValueMetaList();
    	int size = valueMetaList.size()-1;
    	StringBuffer buffer = new StringBuffer("INSERT INTO ");
    	buffer.append(emi.getTableName());
    	buffer.append(" ( ");
    	for (int i = 0; i < size ; i++) {
    		String name = valueMetaList.get(i).getName();
			if(name.equalsIgnoreCase(idColumnName)) continue;
    		if(i > 0)
    			buffer.append(", ");
    		buffer.append(name);
    		sqlInsertColumns.add(name);
    	}
    	String name = valueMetaList.get(size).getName();
    	if(!name.equalsIgnoreCase(idColumnName))
    		buffer.append(", ").append(valueMetaList.get(size).getName());
    	buffer.append(" ) ");
    	buffer.append(" VALUES(");
    	// size -1 becouse the id isn't be inserted
    	for (int i = 0; i < size-1 ; i++)
    		buffer.append("?,");
    	buffer.append('?');
    	buffer.append(" ) ");
    	esdi.sqlInsertColumns=sqlInsertColumns;
    	return buffer.toString();
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        boolean result = super.init(smi, sdi);
        if(!result) return result;
        esdi.database = new Database(this.getParent(), emi.getDatabaseMeta());
        try {
            esdi.database.connect();
        	RowMetaInterface columnsRowMeta = emi.getColumnsRowMeta();
        	esdi.columnsRowMeta = columnsRowMeta;
        	Map<DateDimensionColumn, String> columnNamesForSQL = this.getColumnNamesForSQL();
        	esdi.columnNamesForSQL = columnNamesForSQL;
            String sqlLoad = this.getSqlLoad(columnsRowMeta, columnNamesForSQL);
            esdi.sqlLoad = sqlLoad;
            String sqlInsert = this.getSqlInsert(columnsRowMeta, columnNamesForSQL);
            esdi.sqlInsert = sqlInsert;
            return true;
        } catch (Exception e) {
            logError("Error in initialization of the step " + emi.getName(), e);
            result = false;
            if(esdi.database != null) try{esdi.database.disconnect();}catch(Exception ex) {logError("<WARN> Error to close connection", ex);}
        }
        return result;
    }


}
