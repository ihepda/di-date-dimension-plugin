/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.claudio.dangelo.kettle.plugin.date;

import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 *
 * @author claudio
 */
public class DateDimStepDataInterface extends BaseStepData implements StepDataInterface {

	public Database database;
	public RowMetaInterface columnsRowMeta;
	public String sqlLoad;
	public String sqlInsert;
	public Map<DateDimensionColumn, String> columnNamesForSQL;
	public List<String> sqlLoadColumns;
	public List<String> sqlInsertColumns;
	public boolean isCanceled;
	public Statement pstmtInsert;
	public Statement pstmtLoad;
   
}
