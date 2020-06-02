package com.kafka.algo.runners.datautils;

import static com.kafka.algo.runners.constants.Constants.TOPIC_FIELD_DEFAULT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

public class FieldSelector {
	private static final Logger LOGGER = Logger.getLogger(FieldSelector.class.getName());

	/**
	 * Default Constructor
	 */
	public FieldSelector() {

	}

	/**
	 * @param fieldNameToConsider
	 * @param rec
	 * @return
	 */
	@Deprecated
	public <K, V> long getTimestampFromDataOld(String fieldNameToConsider, ConsumerRecord<K, V> rec) {
		long recTimestamp = 0;
		if (fieldNameToConsider.equals(TOPIC_FIELD_DEFAULT)) {
			recTimestamp = rec.timestamp();
			return recTimestamp;
		} else {
			if (rec.value() instanceof GenericRecord) {
				GenericRecord value = (GenericRecord) rec.value();
				recTimestamp = findField(value, fieldNameToConsider);
				return recTimestamp;
			}
		}
		return recTimestamp;
	}

	/**
	 * @param fieldNameToConsider
	 * @param rec
	 * @return
	 */
	@Deprecated
	public <K, V> long getTimestampFromData(final String fieldNameToConsider, final ConsumerRecord<K, V> rec) {
		long recTimestamp = 0;
		if (fieldNameToConsider.equals(TOPIC_FIELD_DEFAULT)) {
			recTimestamp = rec.timestamp();
			return recTimestamp;
		} else {
			if (rec.value() instanceof GenericRecord) {
				final GenericRecord value = (GenericRecord) rec.value();
				String returnValue = findFieldValue(value.getSchema(), fieldNameToConsider, value);
				try {
					recTimestamp = Long.parseLong(returnValue);
				} catch (NumberFormatException e) {
					recTimestamp = getMillies(returnValue);
				}
				return recTimestamp;
			}
		}
		return recTimestamp;
	}

	/**
	 * @param rec
	 * @param fieldNameToConsider
	 *            : the path for the field should be specified in this. Eg:
	 *            Data,fieldName
	 * @return
	 */
	public <K, V> long getTimestampFromData(final ConsumerRecord<K, V> rec, final String... fieldNameToConsider) {
		long recTimestamp = 0;
		if (fieldNameToConsider[0].equals(TOPIC_FIELD_DEFAULT)) {
			recTimestamp = rec.timestamp();
			return recTimestamp;
		} else {
			if (rec.value() instanceof GenericRecord) {
				recTimestamp = findFieldValue((GenericRecord) rec.value(), fieldNameToConsider);
				return recTimestamp;
			}
		}
		return recTimestamp;
	}

	/**
	 * @param genericRecord
	 * @param collectionName
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private long findFieldValue(final GenericRecord genericRecord, final String... collectionName) {
		long finalValue = 0L;
		Object collectionRec = genericRecord.get(collectionName[0]);
		if (collectionRec instanceof GenericRecord) {
			GenericRecord rec = (GenericRecord) collectionRec;
			try {
				return findFieldValue(rec, removeElement(collectionName, collectionName[0]));
			} catch (Exception e) {
				LOGGER.error("No Field with value of Primitive Data Types For Record: " + rec);
			}

		} else if (collectionRec instanceof GenericArray) {
			GenericArray<GenericRecord> rec = (GenericArray<GenericRecord>) collectionRec;
			try {
				return findFieldValue(rec.get(0), removeElement(collectionName, collectionName[0]));
			} catch (Exception e) {
				LOGGER.error("No Field with value of Primitive Data Types For Record: " + rec);
			}
		} else {
			finalValue = convertToLong(collectionRec);
		}

		return finalValue;

	}

	/**
	 * @param schema
	 * @param name
	 * @param data
	 * @return
	 */
	@Deprecated
	private String findFieldValue(final Schema schema, final String name, final GenericRecord data) {
		String foundField = null;
		if (schema.getField(name) != null) {
			foundField = data.get(name).toString();
			return foundField;
		}
		for (Field field : schema.getFields()) {
			Schema fieldSchema = field.schema();
			if (Type.RECORD.equals(fieldSchema.getType())) {
				foundField = findFieldValue(fieldSchema, name, (GenericRecord) data.get(field.pos()));
				return foundField;
			} else if (Type.ARRAY.equals(fieldSchema.getType())) {
				foundField = findFieldValue(fieldSchema.getElementType(), name, data);
				return foundField;
			} else if (Type.MAP.equals(fieldSchema.getType())) {
				foundField = findFieldValue(fieldSchema.getValueType(), name, data);
				return foundField;
			}
		}

		return foundField;
	}

	/**
	 * @param value
	 * @param name
	 * @return
	 */
	@Deprecated
	private long findField(GenericRecord value, String name) {
		long finalValue = 0;
		Schema schema = value.getSchema();
		if (schema.getField(name) != null) {
			Object returnValue = value.get(name);
			if (returnValue instanceof String) {
				try {
					finalValue = Long.parseLong((String) returnValue);
				} catch (NumberFormatException e) {
					finalValue = getMillies((String) returnValue);
				}
			} else if (returnValue instanceof Long) {
				finalValue = (Long) returnValue;
			} else if (returnValue instanceof Utf8) {
				try {
					finalValue = Long.parseLong(returnValue.toString());
				} catch (NumberFormatException e) {
					finalValue = getMillies(returnValue.toString());
				}
			}
			return finalValue;
		}
		return finalValue;
	}

	/**
	 * @param date
	 * @return
	 */
	private static long getMillies(String date) {

		DateTimeParser[] dateParsers = { DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
				DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
				DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
				DateTimeFormat.forPattern("dd-MM-yyyy").getParser(),
				DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss").getParser(),
				DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss.SSS").getParser(),
				DateTimeFormat.forPattern("ddMMyyyyHHmmss").getParser() };
		DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, dateParsers).toFormatter();

		return formatter.parseDateTime(date).getMillis();
	}

	/**
	 * This will remove the data from a String[]
	 * 
	 * @param n
	 * @param remove
	 * @return
	 */
	private String[] removeElement(String[] n, String remove) {
		final List<String> list = new ArrayList<String>();
		Collections.addAll(list, n);
		list.remove(remove);
		n = list.toArray(new String[list.size()]);
		return n;
	}

	/**
	 * @param collectionRec
	 * @return
	 */
	private long convertToLong(final Object collectionRec) {
		long finalValue = 0L;
		if (collectionRec instanceof String) {
			try {
				finalValue = Long.parseLong((String) collectionRec);
			} catch (NumberFormatException e) {
				finalValue = getMillies((String) collectionRec);
			}
		} else if (collectionRec instanceof Long) {
			finalValue = (Long) collectionRec;
		} else if (collectionRec instanceof Utf8) {
			try {
				finalValue = Long.parseLong(collectionRec.toString());
			} catch (NumberFormatException e) {
				finalValue = getMillies(collectionRec.toString());
			}
		} else if (collectionRec instanceof Integer) {
			finalValue = Long.parseLong(collectionRec.toString());
		} else if (collectionRec instanceof Double) {
			finalValue = Long.parseLong(collectionRec.toString());
		}

		return finalValue;
	}

}
