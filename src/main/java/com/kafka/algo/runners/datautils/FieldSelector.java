package com.kafka.algo.runners.datautils;

import static com.kafka.algo.runners.constants.Constants.TOPIC_FIELD_DEFAULT;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

public class FieldSelector {

	/**
	 * @param fieldNameToConsider
	 * @param rec
	 * @return
	 */
	public static <K, V> long getTimestampFromData(String fieldNameToConsider, ConsumerRecord<K, V> rec) {
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
	 * @param value
	 * @param name
	 * @return
	 */
	private static long findField(GenericRecord value, String name) {
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
			}
			return finalValue;
		}

		// Field foundField = null;
		//
		// for (Field field : schema.getFields()) {
		// Schema fieldSchema = field.schema();
		// Object newVal = value.get(field.name());
		// if (Type.RECORD.equals(fieldSchema.getType())) {
		//
		// foundField = findField(newVal, name);
		// } else if (Type.ARRAY.equals(fieldSchema.getType())) {
		// foundField = findField(fieldSchema.getElementType(), name);
		// } else if (Type.MAP.equals(fieldSchema.getType())) {
		// foundField = findField(fieldSchema.getValueType(), name);
		// }
		//
		// if (foundField != null) {
		// return foundField;
		// }
		// }
		//
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

	// public static void main(String[] args) {
	// String customerSchemaString = "{\"namespace\": \"customer.avro\", \"type\":
	// \"record\", "
	// + "\"name\": \"customer_details\"," + "\"fields\": ["
	// + "{\"name\": \"customer_id\", \"type\": \"string\"},"
	// + "{\"name\": \"dep_id\", \"type\": \"string\"},{\"name\": \"customer_name\",
	// \"type\": \"string\"},{\"name\": \"insert_dt\", \"type\": \"string\"}"
	// + "]}";
	//
	// Schema.Parser parser = new Schema.Parser();
	// Schema schema = parser.parse(customerSchemaString);
	// GenericRecord customerRec = new GenericData.Record(schema);
	// customerRec.put("customer_id", String.valueOf(1));
	// customerRec.put("dep_id", String.valueOf(2));
	// customerRec.put("customer_name", "Customer-" + String.valueOf(3));
	// customerRec.put("insert_dt", String.valueOf(new
	// Timestamp(System.currentTimeMillis())));
	//
	// System.out.println(findField(customerRec, "insert_dt1"));;
	// }

}
