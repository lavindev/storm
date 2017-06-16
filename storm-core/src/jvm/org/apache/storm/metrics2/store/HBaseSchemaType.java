package org.apache.storm.metrics2.store;

enum HBaseSchemaType {
    COMPACT("compact", "org.apache.storm.metrics2.store.HBaseSerializerCompact"),
    EXPANDED("expanded", "org.apache.storm.metrics2.store.HBaseSerializerExpanded");

    private String configKey;
    private String className;

    HBaseSchemaType(String configKey, String className){
        this.configKey = configKey;
        this.className = className;
    }

    public String getClassName(){
        return this.className;
    }

    public static HBaseSchemaType fromKey(String key){
        for (HBaseSchemaType type : HBaseSchemaType.values()) {
            if (type.configKey.equals(key))
                return type;
        }
        throw new IllegalArgumentException("Invalid schema type");
    }

}
