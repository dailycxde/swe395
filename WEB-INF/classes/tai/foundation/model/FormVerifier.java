package tai.foundation.model;

/**
 * @author Ahmet Semiz
 * Creation date: (26.Aðu.2003 08:32:35)
 */

import java.util.*;

import javax.servlet.http.HttpServletRequest;

public class FormVerifier {
	private HashMap fields;
	
	public FormVerifier()	{
		this.setFields(new HashMap());
	}	
	
	public FormField getField(String fieldName)	{
		return( (FormField) this.getFields().get(fieldName));
	}
	
	public FormField addField (String fieldName, int fieldType)	{
		return this.addField(fieldName, fieldType, "","");
	}

	public FormField addField (String fieldName, int fieldType, boolean required)	{
		FormField f = this.addField(fieldName, fieldType, "","");
		f.setRequired(required);
		return f;
	}

	public FormField addField (String fieldName, int fieldType, String minValue, String maxValue)	{
		return (this.addField(fieldName, fieldType, minValue, maxValue, true));
	}

	/**
	 * Method addField.
	 * @param fieldName Form'daki alan ismi
	 * @param fieldType Alan tipi FT_INTEGER, FT_DOUBLE, FT_STRING, FT_DATE
	 * @param minValue Alan FT_INTEGER, FT_DOUBLE, FT_DATE alanlarý için en küçük deðer, FT_STRING için en az uzunluk
	 * @param maxValue Alan FT_INTEGER, FT_DOUBLE, FT_DATE alanlarý için en büyük deðer, FT_STRING için en fazla uzunluk
	 * @param required Bu alan zorunlu bir alan mý?
	 * @return FormField
	 */
	public FormField addField (String fieldName, int fieldType, String minValue, String maxValue, boolean required)	{
		FormField f = new FormField(fieldName, fieldType, minValue, maxValue);
		f.setRequired(required);
		this.getFields().put(fieldName, f);
		return f;
	}
	
	public void verify() throws Hata	{
		
		StringBuffer errorMsg = new StringBuffer();
		for (Iterator iter = this.getFields().values().iterator(); iter.hasNext();) {
			FormField f = (FormField) iter.next();
			try	{
				f.verify();
			} catch (Hata h)	{
				errorMsg.append(h.getMessage());
				errorMsg.append("<BR>");				
			}
		}
		if (errorMsg.length()>0)	{
			throw new Hata(errorMsg.toString());
		}
	}


	public void getFieldValues(HttpServletRequest req)	{

		String fieldValue = null;
		for (Iterator iter = this.getFields().values().iterator(); iter.hasNext();) {
			FormField f = (FormField) iter.next();
			fieldValue =  req.getParameter(f.getFieldName());
			if ((f.getFieldType()==FormField.FT_INTEGER) || (f.getFieldType()==FormField.FT_DOUBLE))	{
				fieldValue =  strRemove(fieldValue, ".,");
			}
			f.setValue(fieldValue);
		}
	}

	
	/**
	 * Returns the fields.
	 * @return HashMap
	 */
	private HashMap getFields() {
		return fields;
	}

	/**
	 * Sets the fields.
	 * @param fields The fields to set
	 */
	private void setFields(HashMap fields) {
		this.fields = fields;
	}

	public final static String strRemove(String str, String fromChrs) {

		if (str == null)	{
			return str;
		}
		StringBuffer result = new StringBuffer();
		char ch;
		for (int i=0;i<str.length();i++)	{
			ch = str.charAt(i);
			if (fromChrs.indexOf(ch) == -1)	{
				result.append(ch);
			}
		}
		return result.toString();
	}

}
