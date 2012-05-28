<?php
/**
 * Zend Framework PgSQL Statement
 *
 * @author    Jenei Viktor Attila // Fixed By Gencer Genç
 * @copyright 2010 Jenei Viktor Attila
 * @license   http://framework.zend.com/license/new-bsd     New BSD License
 */ 
 
/**
 * @see Zend_Db_Statement
 */
require_once 'Zend/Db/Statement.php';

/**
 * Extends for PgSQL
 *
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Statement
 */
class Zend_Db_Statement_Pgsql extends Zend_Db_Statement {
    var $_prepared_sql = '';
    var $_sqlresult = null;
    var $_columnCount = 0;
    var $_rowCount = 0;
    /**
     * @param  string $sql
     * @return void
     * @throws Zend_Db_Statement_Exception
     */
    public function _prepare( $sql ) {
        $this->_prepared_sql = $sql;
    }

    /**
     * Binds a parameter to the specified variable name.
     *
     * @param mixed $parameter Name the parameter, either integer or string.
     * @param mixed $variable  Reference to PHP variable containing the value.
     * @param mixed $type      OPTIONAL Datatype of SQL parameter.
     * @param mixed $length    OPTIONAL Length of SQL parameter.
     * @param mixed $options   OPTIONAL Other options.
     * @return bool
     * @throws Zend_Db_Statement_Exception
     */
    protected function _bindParam( $parameter, &$variable, $type = null, $length = null, $options = null ) {
        return true;
    }

    /**
     * Closes the cursor and the statement.
     *
     * @return bool
     */
    public function close() {
        if ( strlen( $this->_prepared_sql ) > 0 ) {
           $this->_prepared_sql = '';
        }
        if ( $this->_sqlresult ) @pg_free_result( $this->_sqlresult );
        return false;
    }

    /**
     * Closes the cursor, allowing the statement to be executed again.
     *
     * @return bool
     */
    public function closeCursor() {
        if ( $this->_sqlresult ) {
            return @pg_free_result( $this->_sqlresult );
        }
        return false;
    }

    /**
     * Returns the number of columns in the result set.
     * Returns null if the statement has no result set metadata.
     *
     * @return int The number of columns.
     */
    public function columnCount() {
            return $this->_columnCount;
    }

    /**
     * Retrieves the error code, if any, associated with the last operation on
     * the statement handle.
     *
     * @return string error code.
     */
    public function errorCode() {
        return false;        
    }

    /**
     * Retrieves an array of error information, if any, associated with the
     * last operation on the statement handle.
     *
     * @return array
     */
    public function errorInfo() {
        if ( !$this->_prepared_sql ) {
            return false;
        }
        return array(
            1,
            pg_last_error( $this->_adapter->getConnection() )
        );
    }

    /**
     * Executes a prepared statement.
     *
     * @param array $params OPTIONAL Values to bind to parameter placeholders.
     * @return bool
     * @throws Zend_Db_Statement_Exception
     */
    public function _execute(array $params = null) {
        if ( !$this->_prepared_sql ) {
            return false;
        }

        if ($params === null) {
            $params = $this->_bindParam;
        }

        $retval = true;
	    $sql_query = $this->_prepared_sql;
	   // echo $sql_query."\n\n\n";
	        if ( $params ) {
	                foreach ( $params as &$v ) {
	                        $v = ( $v === null ) ? 'NULL' : "'" . pg_escape_string( $this->_adapter->getConnection(), $v ) . "'";
	        }
	        $sql_query = vsprintf( str_replace( "?", "%s", $sql_query ), $params );
	    } else {
	        $sql_query = $sql_query;
	    }
	    $this->_sqlresult = @pg_query( $this->_adapter->getConnection(), $sql_query );
	
	    if ( !$this->_sqlresult ) {
        	$retval = false;
            require_once 'Zend/Db/Statement/Exception.php';
            $error = pg_result_error( $this->_sqlresult );
            if ( !$error) $error = pg_last_error( $this->_adapter->getConnection() );
            
            throw new Zend_Db_Statement_Exception(
                    'PGSql statement execute error : ' . $error,
                    1 );
        }
        $this->_columnCount = pg_field_num( $this->_sqlresult, "" );
        $this->_rowCount = pg_affected_rows( $this->_sqlresult );
        return $retval;
    }

    /**
     * Fetches a row from the result set.
     *
     * @param int $style  OPTIONAL Fetch mode for this fetch operation.
     * @param int $cursor OPTIONAL Absolute, relative, or other.
     * @param int $offset OPTIONAL Number for absolute or relative cursors.
     * @return mixed Array, object, or scalar depending on fetch mode.
     * @throws Zend_Db_Statement_Exception
     */
    public function fetch( $style = null, $cursor = null, $offset = null ) {
        if ( !$this->_sqlresult ) {
            return false;
        }

        if ( $style === null ) {
            $style = $this->_fetchMode;
        }

    $row = null;
    switch ( $style ) {
        case Zend_Db::FETCH_NUM:
        $row = pg_fetch_row( $this->_sqlresult );
        break;
        case Zend_Db::FETCH_ASSOC:
        $row = pg_fetch_assoc( $this->_sqlresult );
        break;
        case Zend_Db::FETCH_BOTH:
        $row = pg_fetch_assoc( $this->_sqlresult );
        if ($row !== false)
            $row = array_merge( $this->_sqlresult, array_values( $row ) );
        break;
        case Zend_Db::FETCH_OBJ:
        $row = pg_fetch_object( $this->_sqlresult );
        break; 
            case Zend_Db::FETCH_BOUND:
                $row = pg_fetch_assoc( $this->_sqlresult );
                if ( $row !== false ){
                    $row = array_merge( $row, array_values( $row ) );
                    $row = $this->_fetchBound( $row );
                }
                break;
        default:
        break;
    }
    return $row;
    }

    /**
     * Retrieves the next rowset (result set) for a SQL statement that has
     * multiple result sets.  An example is a stored procedure that returns
     * the results of multiple queries.
     *
     * @return bool
     * @throws Zend_Db_Statement_Exception
     */
    public function nextRowset()
    {
        /**
         * @see Zend_Db_Statement_Exception
         */
        require_once 'Zend/Db/Statement/Exception.php';
        throw new Zend_Db_Statement_Exception( __FUNCTION__.'() is not implemented' );
    }

    /**
     * Returns the number of rows affected by the execution of the
     * last INSERT, DELETE, or UPDATE statement executed by this
     * statement object.
     *
     * @return int     The number of rows affected.
     */
    public function rowCount() {
        if ( !$this->_sqlresult ) {
            return false;
        }
        return $this->_rowCount;
    }

} 
?>