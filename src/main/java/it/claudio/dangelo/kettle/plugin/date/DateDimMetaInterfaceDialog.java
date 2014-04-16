/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.claudio.dangelo.kettle.plugin.date;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ControlAdapter;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.database.dialog.SQLEditor;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * 
 * @author claudio
 */
public class DateDimMetaInterfaceDialog extends BaseStepDialog implements
		StepDialogInterface {

	private String connectionName;
	private String inputVarName;
	private String idVarname;
	private String tableName;
//	private Map<DateDimensionColumn, String> columnNames;
	private Composite sc;
	
	private DateDimMetaInterface emi;
	private ModifyListener lsMod ;

	public DateDimMetaInterfaceDialog(Shell parent, Object baseStepMeta,
			TransMeta transMeta, String stepname) {
		super(parent, (DateDimMetaInterface) baseStepMeta, transMeta, stepname);
		System.out.println(this);
		emi = (DateDimMetaInterface) baseStepMeta;
//		this.columnNames = new HashMap<DateDimensionColumn, String>();
	}

	public String open() {
		CCombo wConnection;
		Shell parent = getParent();
		Display display = parent.getDisplay();
		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX
				| SWT.MIN);
		shell.setLayout(new FillLayout(SWT.VERTICAL));
		shell.setText("Date dimension output"); //$NON-NLS-1$
		final ScrolledComposite scrollComposite = new ScrolledComposite(shell, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
		//sc = shell;
		//sc = new Composite(shell, SWT.V_SCROLL);
		sc = new Composite(scrollComposite, SWT.NONE);
		//props.setLook(sc);
		
		this.lsMod = new ModifyListener() {
			
			@Override
			public void modifyText(ModifyEvent e) {
				emi.setChanged();
				
			}
		};
		props.setLook(shell);
		setShellImage(shell, emi);
		changed = emi.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		sc.setLayout(formLayout);

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname = new Label(sc, SWT.RIGHT);
		wlStepname.setText("Step Name:"); //$NON-NLS-1$
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(sc, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		wStepname.addModifyListener(lsMod);
		props.setLook(wStepname);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		// Connection line
		wConnection = addConnectionLine(sc, wStepname, middle, margin);
		if (emi.getDatabaseMeta() != null) {
			wConnection.setText(emi.getDatabaseMeta().getName());
			this.connectionName = wConnection.getText();
		}

		if (emi.getDatabaseMeta() == null && transMeta.nrDatabases() == 1) {
			wConnection.select(0);
			this.connectionName = wConnection.getText(); 
		}
		
		wConnection.addModifyListener(new ModifyListener() {
			
			public void modifyText(ModifyEvent e) {
				emi.setChanged();
				connectionName = ((CCombo)e.widget).getText();
			}
		});

		// 
		Control topControl = wConnection;
		try {
			topControl = addInputVarNameLine(middle, margin, topControl);
		} catch (KettleStepException e1) {
			throw new RuntimeException(e1);
		}
		topControl = addVarNameLine(middle, margin, topControl);
		topControl = addTableNameLine(middle, margin, topControl);
		topControl = addTableColumns(middle, margin, topControl);
		topControl = addConversionText(middle, margin, topControl);
		setButtons(margin,topControl);
		
		scrollComposite.setContent(sc);
		scrollComposite.setExpandVertical(true);
		scrollComposite.setExpandHorizontal(true);
		final Composite composite = sc;
		scrollComposite.addControlListener(new ControlAdapter() {
			public void controlResized(ControlEvent e) {
				Rectangle r = scrollComposite.getClientArea();
				scrollComposite.setMinSize(composite.computeSize(r.width, SWT.DEFAULT));
			}
		});

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}

	private Control addInputVarNameLine(int middle, int margin, Control topControl) throws KettleStepException {
		Label wVarNameL = new Label(sc, SWT.RIGHT);
		wVarNameL.setText("Input variable : "); //$NON-NLS-1$
		props.setLook(wVarNameL);
		FormData fdlDocumentNameL = new FormData();
		fdlDocumentNameL.left = new FormAttachment(0, 0);
		fdlDocumentNameL.right = new FormAttachment(middle, -margin);
		fdlDocumentNameL.top = new FormAttachment(topControl, margin);
		wVarNameL.setLayoutData(fdlDocumentNameL);
        RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);
		CCombo combo = new CCombo(sc, SWT.LEFT);
		this.inputVarName = emi.getInputVarName();
        for (int i=0;i<row.size();i++)
        	combo.add(row.getValueMeta(i).getName());
        if(this.inputVarName != null)
        	combo.setText(this.inputVarName);
        /*
        if(ejmi.getFieldName() != null && ejmi.getFieldName().length() > 0)
            fields.setText(ejmi.getFieldName());
        if((ejmi.getFieldName() == null || ejmi.getFieldName().length() == 0) && row.size() == 1)
            fields.setText(row.getValueMeta(0).getName());
            */
        FormData fdFields=new FormData();
        fdFields.left = new FormAttachment(middle, 0);
        fdFields.right = new FormAttachment(100, 0);
        fdFields.top = new FormAttachment(topControl, margin);
        combo.setLayoutData(fdFields);
        props.setLook(combo);
        combo.addModifyListener(new ModifyListener() {
			
			public void modifyText(ModifyEvent e) {
				inputVarName = ((CCombo)e.widget).getText();
			}
		});
		return combo;
	}
	
	private Control addVarNameLine(int middle, int margin, Control topControl) {
		Label wVarNameL = new Label(sc, SWT.RIGHT);
		wVarNameL.setText("Id date variable name : "); //$NON-NLS-1$
		props.setLook(wVarNameL);
		FormData fdlDocumentNameL = new FormData();
		fdlDocumentNameL.left = new FormAttachment(0, 0);
		fdlDocumentNameL.right = new FormAttachment(middle, -margin);
		fdlDocumentNameL.top = new FormAttachment(topControl, margin);
		wVarNameL.setLayoutData(fdlDocumentNameL);
		Text wVarName = new Text(sc, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		this.idVarname = emi.getIdVarName();
		if(this.idVarname != null)
			wVarName.setText(this.idVarname);
		props.setLook(wVarName);
		wVarName.addModifyListener(new ModifyListener() {
			
			public void modifyText(ModifyEvent e) {
				idVarname = ((Text)e.widget).getText();
			}
		});
		FormData fdlDocumentName = new FormData();
		fdlDocumentName.left = new FormAttachment(middle, 0);
		fdlDocumentName.right = new FormAttachment(100, 0);
		fdlDocumentName.top = new FormAttachment(topControl, margin);
		wVarName.setLayoutData(fdlDocumentName);
		wVarName.addModifyListener(lsMod);
		return wVarName;
	}
	
	private Control addTableNameLine(int middle, int margin, Control topControl) {
		Label wTableNameL = new Label(sc, SWT.RIGHT);
		wTableNameL.setText("Table name : "); //$NON-NLS-1$
		props.setLook(wTableNameL);
		FormData fdlFieldL = new FormData();
		fdlFieldL.left = new FormAttachment(0, 0);
		fdlFieldL.right = new FormAttachment(middle, -margin);
		fdlFieldL.top = new FormAttachment(topControl, margin);
		wTableNameL.setLayoutData(fdlFieldL);
		Text wTableName = new Text(sc, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		this.tableName = emi.getTableName();
		if(this.tableName != null)
			wTableName.setText(this.tableName);
		props.setLook(wTableName);
		wTableName.addModifyListener(new ModifyListener() {
			
			public void modifyText(ModifyEvent e) {
				tableName = ((Text)e.widget).getText();
			}
		});
		FormData fdlDocumentName = new FormData();
		fdlDocumentName.left = new FormAttachment(middle, 0);
		fdlDocumentName.right = new FormAttachment(100, 0);
		fdlDocumentName.top = new FormAttachment(topControl, margin);
		wTableName.setLayoutData(fdlDocumentName);
		wTableName.addModifyListener(lsMod);
		return wTableName;
	}
	
	private Label wlColumns;
	private TableView wColumns;
	private FormData fdlColumns, fdColumns;
	private ColumnInfo[] ciKey;
	private Control addTableColumns(int middle, int margin, Control topControl) {
//		this.columnNames = emi.getColumns();
		Map<DateDimensionColumn, String> columns = emi.getColumns();
		if(columns != null && !columns.containsKey(DateDimensionColumn.ID)) columns.put(DateDimensionColumn.ID, DateDimensionColumn.ID.name());

		wlColumns=new Label(sc, SWT.NONE);
		wlColumns.setText("Column names :"); //$NON-NLS-1$
 		props.setLook(wlColumns);
 		fdlColumns=new FormData();
 		fdlColumns.left  = new FormAttachment(0, 0);
 		fdlColumns.top   = new FormAttachment(topControl, margin);
 		wlColumns.setLayoutData(fdlColumns);

		int nrKeyCols=2;
		int nrKeyRows=(columns!=null?columns.size():1);

		ciKey=new ColumnInfo[nrKeyCols];
		DateDimensionColumn[] values = DateDimensionColumn.values();
		String[] columnsType = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			columnsType[i] = values[i].name();
		}
		ciKey[0]=new ColumnInfo("Column type",  ColumnInfo.COLUMN_TYPE_CCOMBO, columnsType, false);
		ciKey[1]=new ColumnInfo("Column name",  ColumnInfo.COLUMN_TYPE_TEXT); //$NON-NLS-1$

		wColumns=new TableView(transMeta, sc, 
			      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, 
			      ciKey, 
			      nrKeyRows,  
			      null,
				  props
			      );
		wColumns.addModifyListener(lsMod);
		if(columns != null) {
			Set<DateDimensionColumn> keySet = columns.keySet();
			int i = 0;
			for (DateDimensionColumn columnType : keySet) {
				String value = columns.get(columnType);
				TableItem item = wColumns.table.getItem(i++);
				item.setText(1, columnType.name());
				item.setText(2, value);
			}
		}
		fdColumns=new FormData();
		fdColumns.left  = new FormAttachment(0, 0);
		fdColumns.top   = new FormAttachment(wlColumns, margin);
		fdColumns.right = new FormAttachment(100, 0);
		fdColumns.height = 200; 
//		fdParam.bottom= new FormAttachment(wOK, -2*margin);
		wColumns.setLayoutData(fdColumns);
		
		
//		topControl = this.addTableColumnLine("Id column :", DateDimensionColumn.ID, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Timestamp column :", DateDimensionColumn.TIMESTAMP, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Year column :", DateDimensionColumn.YEAR, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Month column :", DateDimensionColumn.MONTH, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Day column :", DateDimensionColumn.DAY, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Hour column :", DateDimensionColumn.HOUR, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Minute column :", DateDimensionColumn.MINUTE, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Day of week column :", DateDimensionColumn.DAY_OF_WEEK, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Week column :", DateDimensionColumn.WEEK, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Two months column :", DateDimensionColumn.TWO_MONTHS, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Tree months column :", DateDimensionColumn.TREE_MONTHS, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Four months column :", DateDimensionColumn.FOUR_MONTHS, middle, margin, topControl);
//		topControl = this.addTableColumnLine("Six months column :", DateDimensionColumn.SIX_MONTHS, middle, margin, topControl);
		return wColumns;
	}
	
	private Label        wlPropName;
	private StyledTextComp   wPropName;
	private FormData     fdlPropName, fdPropName;
	private Control addConversionText(int middle, int margin, Control topControl) {
		wlPropName=new Label(sc, SWT.NONE);
		wlPropName.setText("Name conversion"); //$NON-NLS-1$
 		props.setLook(wlPropName);
 		fdlPropName=new FormData();
 		fdlPropName.left = new FormAttachment(0, 0);
 		fdlPropName.top  = new FormAttachment(topControl, margin*2);
 		wlPropName.setLayoutData(fdlPropName);

 		wPropName=new StyledTextComp(variables, sc, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "");
 		props.setLook(wPropName, Props.WIDGET_STYLE_FIXED);
 		fdPropName=new FormData();
 		fdPropName.left  = new FormAttachment(0, 0);
 		fdPropName.top   = new FormAttachment(wlPropName, margin  );
 		fdPropName.right = new FormAttachment(100, 0);
// 		fdPropName.bottom= new FormAttachment(60, 0     );
 		fdPropName.height = 200;
 		wPropName.setLayoutData(fdPropName);
 		try {
			wPropName.setText(emi.conversionString());
		} catch (KettleException e) {
			this.showMessageError("Error", "Error to retrieve convertiosn informations");
			e.printStackTrace();
		}
 		wPropName.addModifyListener(lsMod);
 		return wPropName;
	}

	
	/*
	private Control addTableColumnLine(String label, final DateDimensionColumn column, int middle, int margin, Control topControl) {
		Label wColumnNameL = new Label(sc, SWT.RIGHT);
		wColumnNameL.setText(label); //$NON-NLS-1$
		props.setLook(wColumnNameL);
		FormData fdlFieldL = new FormData();
		fdlFieldL.left = new FormAttachment(0, 0);
		fdlFieldL.right = new FormAttachment(middle, -margin);
		fdlFieldL.top = new FormAttachment(topControl, margin);
		wColumnNameL.setLayoutData(fdlFieldL);
		Text wColumnName = new Text(sc, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		String value = this.columnNames.get(column);
		wColumnName.setText((value != null? value : ""));
		props.setLook(wColumnName);
		wColumnName.addModifyListener(new ModifyListener() {
			
			public void modifyText(ModifyEvent e) {
				String columnName = ((Text)e.widget).getText();
				if(columnName == null || columnName.length() == 0)
					columnNames.remove(column);
				else
					columnNames.put(column, columnName);
			}
		});
		FormData fdlDocumentName = new FormData();
		fdlDocumentName.left = new FormAttachment(middle, 0);
		fdlDocumentName.right = new FormAttachment(100, 0);
		fdlDocumentName.top = new FormAttachment(topControl, margin);
		wColumnName.setLayoutData(fdlDocumentName);
		return wColumnName;
		
	}

*/
	private void ok()  {
		stepname = wStepname.getText();
		setInfos();
		this.dispose();
	}

	private void setButtons(int margin, Control topControl) {
//		Composite composite = new Composite(shell, SWT.NONE);
//		composite.setLayout(new RowLayout());
		// Some buttons
		wOK = new Button(sc, SWT.PUSH);
		wOK.setText("OK"); //$NON-NLS-1$
		wPreview = new Button(sc, SWT.PUSH);
		wPreview.setText("SQL"); //$NON-NLS-1$
		wCancel = new Button(sc, SWT.PUSH);
		wCancel.setText("Cancel"); //$NON-NLS-1$
//		setButtonPositions(new Button[] { wOK, wCancel, wPreview }, margin,
//				null);
		BaseStepDialog.positionBottomButtons(sc, new Button[] { wOK, wCancel, wPreview }, margin, topControl);
		wOK.addListener(SWT.Selection, new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		});
		wCancel.addListener(SWT.Selection, new Listener() {
			public void handleEvent(Event e) {
				stepname = null;
				emi.setChanged(changed);
				dispose();
			}
		});
		wPreview.addListener(SWT.Selection, new Listener() {
			public void handleEvent(Event e) {
				sql();
			}
		});
	}

	private void setInfos() {
		checkData();
		emi.setDatabaseMeta(transMeta.findDatabase(connectionName));
		emi.setIdVarName((idVarname!= null && idVarname.length()>0? idVarname: null));
		emi.setInputVarName((inputVarName!= null && inputVarName.length()>0? inputVarName: null));
		emi.setTableName((tableName!= null && tableName.length()>0? tableName: null));
		HashMap<DateDimensionColumn, String> columnNames = new HashMap<DateDimensionColumn, String>();
		int itemCount = wColumns.getItemCount();
		for (int i = 0; i < itemCount; i++) {
			String[] item = wColumns.getItem(i);
			if(item[0] == null || item[0].length() == 0) continue;
			DateDimensionColumn columnType = DateDimensionColumn.valueOf(item[0]);
			if(item[1] == null || item[1].length() == 0) item[1] = item[0];
			columnNames.put(columnType, item[1]);
		}
		emi.setColumns(columnNames);
		try {
			emi.popolateConversion(this.wPropName.getText());
		} catch (KettleException e) {
			this.showMessageError("Invalid conversion infos", "There are errors in the conversion text area. Please correct first");
			e.printStackTrace();
		}
	}

	private void sql() {
		if(!checkData())
			return;
		this.setInfos();
		DatabaseMeta dbMeta = transMeta.findDatabase(connectionName);
		String sql = emi.getDDL();
		SQLEditor sqledit = new SQLEditor(shell, SWT.NONE, dbMeta, transMeta.getDbCache(), sql);
		sqledit.open();

	}

	private void showMessageError(String title, String message) {
		MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
		mb.setMessage(title); //$NON-NLS-1$
		mb.setText(message); //$NON-NLS-1$
		mb.open();
		return;
	}
	
	private boolean checkData() {
		if(this.connectionName == null) {
			showMessageError("Select one valid connection", "Select one valid connection");
			return false;
		} else if(this.idVarname == null) {
			showMessageError("Invalid id variable name", "You must insert a name of a variable where the dimension id must be saved");
			return false;
		} else if(this.inputVarName == null) {
			showMessageError("Invalid input variable name", "You must insert the name of the variable that contains the date value");
			return false;
		} else if(this.tableName == null) {
			showMessageError("Invalid table name", "You must insert the name of the table dimension");
			return false;
		} 
		return true;
	}
	
}
