package ru.db;

public class Start {

    public static void main(String[] args) throws Exception {
        NoHeapDB db = new NoHeapDB();
        db.createStore(
                "MyTestDataStore",
                DataStore.Storage.PERSISTED, //or DataStore.Storage.PERSISTED
                10);

        db.putShort("MyTestDataStore", "Test", (short) 34);
        db.putShort("MyTestDataStore", "Test2", (short) 44);
        db.putShort("MyTestDataStore", "Test3", (short) 100);

        db.remove("MyTestDataStore", "Test2");

        System.out.println(db.getShort("MyTestDataStore", "Test"));
    }
}
