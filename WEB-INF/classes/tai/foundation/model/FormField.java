package tai.foundation.model;

import java.util.*;
import java.text.*;

/**
 * @author Ahmet Semiz
 * Creation date: (26.Aðu.2003 08:32:27)
 */
public class FormField {
	private String fieldName;
	private int fieldType;
	private String value;
	private String minValue;
	private String maxValue;
	private boolean required;

	public static final int FT_INTEGER	= 1;
	public static final int FT_DOUBLE		= 2;
	public static final int FT_STRING 		= 3;
	public static final int FT_DATE			= 4;
	

	public FormField(
		String fieldName,
		int fieldType,
		String minValue,
		String maxValue) {
		super();
		this.setRequired(true);
		this.setFieldName(fieldName);
		this.setFieldType(fieldType);
		this.setMinValue(minValue);
		this.setMaxValue(maxValue);
		if ((this.getFieldType() == FT_INTEGER) || (this.getFieldType() == FT_DOUBLE))	{//Sayýsal deðerler negatif olmasýn
			if (this.getMinValue().equals(""))	{
				this.setMinValue("0");
			}
		}
	}

	/**
	 * Returns the value.
	 * @return String
	 */
	private String getValue() {
		return value;
	}

	/**
	 * Sets the value.
	 * @param value The value to set
	 */
	public void setValue(String value) {
		this.value = value;
	}

	public String strValue() {
		return (this.getValue() == null ? "" : this.getValue());
	}

	public int intValue() {
		try {
			return (Integer.parseInt(this.getValue()));
		} catch (Exception e) {
			return 0;
		}
	}

	public double dblValue() {
		try {
			return (Double.parseDouble(this.getValue()));
		} catch (Exception e) {
			return 0;
		}
	}

	public Date dateValue() {
		try {
			return (dateFormatter.parse(this.getValue()));	
		} catch (ParseException e) {
			return null;
		}		
	}
	
	/**
	 * Returns the minValue.
	 * @return String
	 */
	public String getMinValue() {
		return minValue;
	}

	/**
	 * Sets the minValue.
	 * @param minValue The minValue to set
	 */
	public void setMinValue(String minValue) {
		this.minValue = minValue;
	}

	/**
	 * Returns the maxValue.
	 * @return String
	 */
	public String getMaxValue() {
		return maxValue;
	}

	/**
	 * Sets the maxValue.
	 * @param maxValue The maxValue to set
	 */
	public void setMaxValue(String maxValue) {
		this.maxValue = maxValue;
	}

	/**
	 * Returns the fieldName.
	 * @return String
	 */
	public String getFieldName() {
		return fieldName;
	}

	/**
	 * Sets the fieldName.
	 * @param fieldName The fieldName to set
	 */
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	/**
	 * Returns the fieldType.
	 * @return int
	 */
	public int getFieldType() {
		return fieldType;
	}

	/**
	 * Sets the fieldType.
	 * @param fieldType The fieldType to set
	 */
	public void setFieldType(int fieldType) {
		this.fieldType = fieldType;
	}

	/**
	 * Returns the required.
	 * @return boolean
	 */
	public boolean isRequired() {
		return required;
	}

	/**
	 * Sets the required.
	 * @param required The required to set
	 */
	public void setRequired(boolean required) {
		this.required = required;
	}

	public void verify() throws Hata {
		if (this.isRequired() || ((this.getValue() != null) && !this.getValue().equals(""))) {
			switch (this.getFieldType()) {
				case FT_INTEGER :
					try {
						int val = Integer.parseInt(this.getValue());
						if (!this.getMinValue().equals("")) {
							if (val < Integer.parseInt(this.getMinValue())) {
								throw new Hata(formatStrMin.format(new Object[] {this.getFieldName(), this.getMinValue(),""}));
							}
						}
						if (!this.getMaxValue().equals("")) {
							if (val > Integer.parseInt(this.getMaxValue())) {
								throw new Hata(formatStrMax.format(new Object[] {this.getFieldName(), this.getMaxValue(),""}));
							}
						}
					} catch (Exception e) {
						throw new Hata(formatStrInvalid.format(new Object[] {this.getFieldName(), "SAYISAL"}));
					}
					break;
				case FT_DOUBLE :
					try {
						double val = Double.parseDouble(this.getValue());
						if (!this.getMinValue().equals("")) {
							if (val < Double.parseDouble(this.getMinValue())) {
								throw new Hata(formatStrMin.format(new Object[] {this.getFieldName(), this.getMinValue(),""}));
							}
						}
						if (!this.getMaxValue().equals("")) {
							if (val > Double.parseDouble(this.getMaxValue())) {
								throw new Hata(formatStrMax.format(new Object[] {this.getFieldName(), this.getMaxValue(),""}));
							}
						}
					} catch (Exception e) {
						throw new Hata(formatStrInvalid.format(new Object[] {this.getFieldName(), "SAYISAL"}));
					}
					break;

				case FT_DATE :
					try {
						Date dt = dateFormatter.parse(this.getValue());
						if (!this.getMinValue().equals("")) {
							if (dt.before(dateFormatter.parse(this.getMinValue()))) {
								throw new Hata(formatStrMin.format(new Object[] {this.getFieldName(), this.getMinValue(),""}));
							}
						}
						if (!this.getMaxValue().equals("")) {
							if (dt.after(dateFormatter.parse(this.getMaxValue()))) {
								throw new Hata(formatStrMax.format(new Object[] {this.getFieldName(), this.getMaxValue(),""}));
							}
						}
					} catch (Exception e) {
						throw new Hata(formatStrInvalid.format(new Object[] {this.getFieldName(), "TARÝH"}));
					}
					break;

				case FT_STRING :
					if ((this.getValue() == null)	|| (this.getValue().equals(""))) {
						throw new Hata(formatStrInvalid.format(new Object[] {this.getFieldName(), "KARAKTER"}));
					}
					if (!this.getMinValue().equals("")) {
						if (this.getValue().length()	< Integer.parseInt(this.getMinValue())) {
								throw new Hata(formatStrMin.format(new Object[] {this.getFieldName(), this.getMinValue(),"KARAKTER UZUNLUÐUNDA"}));
						}
					}
					if (!this.getMaxValue().equals("")) {
						if (this.getValue().length()	> Integer.parseInt(this.getMaxValue())) {
								throw new Hata(formatStrMax.format(new Object[] {this.getFieldName(), this.getMaxValue(),"KARAKTER UZUNLUÐUNDA"}));
						}
					}
					break;

				default :
					throw new Hata(
						this.getFieldName() + " ALAN TÝPÝ BELÝRLENEMEDÝ!");
			}
		}
	}


	private static MessageFormat formatStrInvalid = new MessageFormat("\"{0}\" {1} ALANI DEÐERÝ GEÇERSÝZ!");
	private static MessageFormat formatStrMin = new MessageFormat("\"{0}\" ALANI EN AZ {1} {2} OLMALIDIR!");
	private static MessageFormat formatStrMax = new MessageFormat("\"{0}\" ALANI EN FAZLA {1} {2} OLABÝLÝR!");
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat("dd.MM.yyyy");			
}
