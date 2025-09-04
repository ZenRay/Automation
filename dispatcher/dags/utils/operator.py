#coding:utf-8
"""Airflow Operator
* MaxcomputeOperator, Maxcompute Operator
"""
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from utils.hooks import MaxcomputeHook, LarkSheetsHook

logger = logging.getLogger("dags.utils.operator")

class MaxcomputeOperator(BaseOperator):
    """
    MaxCompute Operator
    
    Executes SQL statements on MaxCompute using MaxcomputeHook.
    """
    
    @apply_defaults
    def __init__(self, 
                 sql: str,
                 hints=None,
                 conn_id: str = 'maxcompute_dev',
                 *args, **kwargs):
        """
        Initialize MaxCompute Operator
        
        Args:
            sql: SQL statement to execute
            conn_id: Airflow connection ID for MaxCompute
        """
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.hints = hints
        self.hook = None

    def execute(self, context):
        """
        Execute the SQL statement on MaxCompute
        
        Args:
            context: Airflow execution context
        """
        logger.info(f"Executing SQL [Maxcompute] Start")
        
        if self.hook is None:
            self.hook = MaxcomputeHook(conn_id=self.conn_id)
        
        self.hook.execute_sql(self.sql, hints=self.hints)
        
        logger.info(f"Executing SQL [Maxcompute] Success: \n{self.sql}")
        
    
    
class LarkSheetsOperator(BaseOperator):
    """Lark Sheets Operator
    
    Uploads data to Lark Sheets using LarkSheetsHook.
    """
    
    @apply_defaults
    def __init__(self, 
                 file_path: str,
                 range_str: str,
                 sheet_title: str,
                 target_url: str,
                 conn_id: str = 'lark_app',
                 upload_task_type:str = "single2Single",
                 *args, **kwargs):
        """
        Initialize Lark Sheets Operator
        
        Args:
            file_path: str or List[str], Path to the file to load data from
            range_str: str or List[str], Cell range in A1 notation. 
                If task_type is "Batch", must be a list of ranges.
            sheet_title: str or List[str], Title/Id of the sheet to upload data to.
                If task_type is "Batch", can be a list of titles/Ids, or a single title/Id if all data goes to one sheet.
            target_url: URL of the target Lark Sheet
            conn_id: Airflow connection ID for Lark Sheets
            upload_task_type: Type of task, None, "single" or "batch". Default is "single2Single".
                If "Single2Single", uploads data to one sheet. Single file and single range are expected.
                If "Single2Batch",  uploads data to multi range. Single file and multi range are expected
                    and 'range_str' must be list with multiple ranges.
        """
        super().__init__(*args, **kwargs)

        self.file_path = file_path
        self.range_str = range_str
        self.sheet_title = sheet_title
        self.target_url = target_url
        self.conn_id = conn_id
        self.hook = None

        # TODO: Add Download Task Type
        if upload_task_type is not None and upload_task_type not in ["single", "batch"]:
            raise ValueError("upload_task_type must be either 'single' or 'batch' or None.")
        
        self.upload_task_type = upload_task_type
        
        
    
    def execute(self, context):
        """
        Execute the data upload to Lark Sheets
        
        Args:
            context: Airflow execution context
        """
        logger.info(f"Uploading data to Lark Sheets Start")
        
        if self.hook is None:
            self.hook = LarkSheetsHook(conn_id=self.conn_id, target_url=self.target_url)
        
        if self.upload_task_type.startswith("Single"):
            df = self.hook.load_data(self.file_path)
        
        # Single File and Single 
        if self.upload_task_type == "Single2Single":
            # Get context args columns
            columns = context['params'].get('columns', df.columns.tolist())
            
            self.hook.extract_data2sheet_values(
                df=df,
                columns=columns,
                range_str=self.range_str,
                sheet_title=self.sheet_title,
                target_url=self.target_url
            )
        elif self.upload_task_type == "Single2Batch":
            
            if self.__is_single_conf("sheet_title"):
                for process_columns, process_ranges in zip(columns, range_str):
                    self.hook.extract_data2sheet_values(
                        df=df
                        ,columns=process_columns
                        ,range_str=process_ranges
                        ,sheet_title=sheet_title
                    )
            
        else:
            raise ValueError("task_type must be either 'single' or 'batch'.")
        
        logger.info(f"Uploading data to Lark Sheets Success")
        
        
    
    def __is_single_conf(self, property:str, types=(str,)):
        """Check Single Configuration Validation

        Raises:
            ValueError: If parameters are not valid for single upload
        """
        if not isinstance(getattr(self, property), types):
            raise ValueError(f"For single upload job, '{property}' must be a single string.")

        return True

    def __is_multi_conf(self, property:str, is_nested=False):
        """Check Multi Configuration Validation
        
        Raises:
            ValueError: If parameters are not valid for multi upload
        """
        value = getattr(self, property)
        if  not isinstance(value, (list, tuple)) and not is_nested:
            raise ValueError(
                f"For Non-nested Batch job, expected 'list' or 'tuple', but get '{type(value)}'"
            )
        
        if not (
            isinstance(value, (list, tuple))
                and isinstance(value[0], (list, tuple))
        ) and is_nested:
            if isinstance(value, (list, tuple)):
                msg = f"List[Non-List]"
            else:
                msg = "Non-List"
            raise ValueError(
                f"For nested Batch job, expected List[List], but{msg} "
            )
            
        return True