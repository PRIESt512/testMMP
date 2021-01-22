package ru.db.core;

import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.stack.mutable.primitive.LongArrayStack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.db.DataStore;
import ru.db.HashBase;
import ru.db.NoHeapDBStore;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public abstract class DbCore implements DataStore {
    private static final Logger log = LoggerFactory.getLogger(DbCore.class);

    protected static final int MEGABYTE = 1024 * 1024;
    protected static final int JOURNAL_SIZE_FACTOR = 100;
    protected static final int DEFAULT_JOURNAL_SIZE = MEGABYTE * JOURNAL_SIZE_FACTOR;

    protected int bufferSize = DEFAULT_JOURNAL_SIZE;
    protected int recordCount;
    protected long currentEnd = 0;

    // Индекс всех активных записей в файле хранилища
    protected HashBase index;

    // Буфер, с помощью которого осуществляются все операции с хранилищем (вставка, удаление и т.д.)
    protected ByteBuffer buffer;

    /**
     * key - размер пустого блока, пригодного для записи
     * value - список указателей (смещений) на пустые блоки
     **/
    protected Map<Integer, LongArrayStack> emptyIdx = new TreeMap<>();

    protected DbCore() {
        index = createIndexJournal();
        buffer = createBuffer();
    }

    @Override
    public boolean putInteger(String key, Integer val) {
        return putVal(key, val, INT_RECORD_TYPE);
    }

    @Override
    public Integer getInteger(String key) {
        return null;
    }

    @Override
    public boolean putShort(String key, Short val) {
        return putVal(key, val, SHORT_RECORD_TYPE);
    }

    @Override
    public Short getShort(String key) {
        return null;
    }

    @Override
    public boolean putLong(String key, Long val) {
        return false;
    }

    @Override
    public Long getLong(String key) {
        return null;
    }

    @Override
    public boolean putFloat(String key, Float val) {
        return false;
    }

    @Override
    public Float getFloat(String key) {
        return null;
    }

    @Override
    public boolean putDouble(String key, Double val) {
        return false;
    }

    @Override
    public Double getDouble(String key) {
        return null;
    }

    @Override
    public boolean putString(String key, String val) {
        return false;
    }

    @Override
    public String getString(String key) {
        return null;
    }

    @Override
    public boolean putObject(String key, Object msg) {
        return false;
    }

    @Override
    public Object getObject(String key) {
        return null;
    }

    @Override
    public boolean putChar(String key, char val) {
        return false;
    }

    @Override
    public char getChar(String key) {
        return 0;
    }

    @Override
    public boolean remove(String key) {
        return false;
    }

    protected Optional<Object> getValue(String key, byte type) {
        var offset = getRecordOffset(key);
        if (offset != null && offset > -1) {
            return getValue(offset, type);
        }
        return Optional.empty();
    }

    protected Optional<Object> getValue(Long offset, byte type) {
        if (offset == null || offset <= -1) {
            return Optional.empty();
        }
        // Перейти к смещению этой записи в файле журнала
        buffer.position(offset.intValue());

        if (!readHeader(type)) {
            return Optional.empty();
        }

        return readValue(type);
    }

    private boolean readHeader(byte type) {
        return isActiveBlock() && checkValueType(type);
    }

    private boolean isActiveBlock() {
        byte active = buffer.get();
        return active == ACTIVE_RECORD;
    }

    private boolean checkValueType(byte type) {
        byte typeStored = buffer.get();
        return type == typeStored;
    }

    private Optional<Object> readValue(byte type) {
        Object val = null;
        int dataLength = buffer.getInt();
        byte[] bytes;

        switch (type) {
            case LONG_RECORD_TYPE:
                val = buffer.getLong();
                break;
            case INT_RECORD_TYPE:
                val = buffer.getInt();
                break;
            case DOUBLE_RECORD_TYPE:
                val = buffer.getDouble();
                break;
            case FLOAT_RECORD_TYPE:
                val = buffer.getFloat();
                break;
            case SHORT_RECORD_TYPE:
                val = buffer.getShort();
                break;
            case CHAR_RECORD_TYPE:
                val = buffer.getChar();
                break;
            case BYTEARRAY_RECORD_TYPE:
                bytes = new byte[dataLength];
                buffer.get(bytes);
                val = bytes;
                break;
            case TEXT_RECORD_TYPE:
                bytes = new byte[dataLength];
                buffer.get(bytes);
                val = new String(bytes);
                break;
            default:
                throw new IllegalArgumentException("Неподдерживаемый тип данных для записи");
        }
        return Optional.of(val);
    }

    /**
     * Каждое сообщение записывается в файл со следующей структурой записей:
     * <p>
     * HEADER:
     * Boolean - Индикатор активной записи
     * Byte - тип сообщения (0 = пусто, 1 = байты, 2 = строка)
     * Integer - размер полезной нагрузки записи (не заголовок)
     * <p>
     * DATA:
     * Массив байтов или значение данных - полезная нагрузка сообщения
     *
     * @param key  ключ, по которому нужно сделать запись
     * @param val  значение, которое нужно сохранить
     * @param type тип сохраняемого значения
     * @return <code>true</code> если успешно, <code>false</code> иначе
     */
    protected boolean putVal(String key, Object val, byte type) {
        synchronized (this) {
            int datalen;

            switch (type) {
                case LONG_RECORD_TYPE:
                    datalen = Long.BYTES;
                    break;
                case INT_RECORD_TYPE:
                    datalen = Integer.BYTES;
                    break;
                case DOUBLE_RECORD_TYPE:
                    datalen = Double.BYTES;
                    break;
                case FLOAT_RECORD_TYPE:
                    datalen = Float.BYTES;
                    break;
                case SHORT_RECORD_TYPE:
                    datalen = Short.BYTES;
                    break;
                case CHAR_RECORD_TYPE:
                    datalen = 2; // 16-bit Unicode character
                    break;
                case TEXT_RECORD_TYPE:
                    datalen = ((String) val).getBytes().length;
                    break;
                case BYTEARRAY_RECORD_TYPE:
                    datalen = ((byte[]) val).length;
                    break;
                default:
                    throw new IllegalArgumentException("Неподдерживаемый тип данных для записи");
            }

            var location = setNewRecordLocation(datalen);

            writeHeader(type, datalen);
            writeValue(type, val);

            /*
            Проверяем, нужно ли нам добавлять пустой блок после записи данных, если мы вставили эту новую запись в пустой блок,
            найденный в setNewRecordLocation
             */
            if (location.newEmptyRecordSize != -1) {
                buffer.put(INACTIVE_RECORD);
                buffer.put(EMPTY_RECORD_TYPE);
                buffer.putInt(location.newEmptyRecordSize);

                if (buffer.position() > currentEnd) {
                    currentEnd = buffer.position();
                }
            }

            indexRecord(key, location.offset);
            recordCount++;

            return true;
        }
    }

    private void writeHeader(byte type, int datalen) {
        buffer.put(ACTIVE_RECORD);
        buffer.put(type);
        buffer.putInt(datalen);
    }

    private void writeValue(byte type, Object val) {
        switch (type) {
            case LONG_RECORD_TYPE:
                buffer.putLong((Long) val);
                break;
            case INT_RECORD_TYPE:
                buffer.putInt((Integer) val);
                break;
            case DOUBLE_RECORD_TYPE:
                buffer.putDouble((Double) val);
                break;
            case FLOAT_RECORD_TYPE:
                buffer.putFloat((Float) val);
                break;
            case SHORT_RECORD_TYPE:
                buffer.putShort((Short) val);
                break;
            case CHAR_RECORD_TYPE:
                buffer.putChar((char) val);
                break;
            case TEXT_RECORD_TYPE:
                buffer.put(((String) val).getBytes());
                break;
            case BYTEARRAY_RECORD_TYPE:
                buffer.put((byte[]) val);
                break;
            default:
                throw new IllegalArgumentException("Неподдерживаемый тип данных для записи");
        }
    }

    protected boolean indexRecord(String key, long recordLocation) {
        return index.put(key, recordLocation);
    }

    protected Long getRecordOffset(String key) {
        return index.get(key);
    }

    /**
     * Поиск свободного блока для размещения новой записи
     *
     * @param dataLen размер данных, который нужно записать
     * @return место, куда можно записать блок данных
     */
    protected JournalLocationData setNewRecordLocation(int dataLen) {
        int recordSize = NoHeapDBStore.Header.HEADER_SIZE + dataLen;

        /*
        Всегда сохранять сообщения в конце журнала, если
        в журнале есть достаточно большая пустая позиция
        */
        var location = getStorageLocation(dataLen);
        if (location.offset == -1) {
           /*
            Ничего не найдено, нужно добавить запись в конец журнала
            Ищите там сейчас, только если мы еще не там
            */
            long currentPos = buffer.position();
            if (currentPos != currentEnd) {
                currentPos = buffer.position((int) currentEnd).position();
            }

            long journalLen = getJournalLen();
            if ((currentPos + recordSize) >= journalLen) {
                currentPos = expandJournal(journalLen, currentPos);
            }

        } else {
            buffer.position((int) location.offset);
        }

        return location;
    }

    /**
     * @param recordLength длина нового блока для записи (HEADER_SIZE + длина записываемых данных)
     */
    protected JournalLocationData getStorageLocation(int recordLength) {
        var location = new JournalLocationData();
        location.offset = -1;   // куда писать новую запись
        location.newEmptyRecordSize = -1; // оставшаяся часть от пустого места

        // Проверить, если в списке пустые блоки
        if (emptyIdx == null || emptyIdx.isEmpty()) {
            return location;
        }

        return blockMarking(location, recordLength);
    }

    protected abstract long getJournalLen();

    /**
     * @param journalLen
     * @param currentPos
     * @return
     */
    protected abstract long expandJournal(long journalLen, long currentPos);

    /**
     * Определить, если ли свободный блок для записи нужного размера.
     * Есть несколько критериев.
     * 1. Свободный блок должен точно соответствовать блоку с учетом размера записываемых данных и размера заголовка
     * (заменяя заголовок существующей пустой записи)
     * 2. Либо, если пустая запись больше, она должна быть достаточно большой, чтобы вместить заголовок и
     * данные новой записи, а также для пометки другого заголовка; Это необходимо для последующего разделения исходного
     * блока данных на 2 отдельных блока, чтобы 2 блок мог в дальнейшем использоваться для других записей. Минимальный
     * размер 2 блока в таком случае должен быть равен HEADER_SIZE + 1, где 1 - минимальный размер для хранения данных (byte)
     * <p>
     * Другими словами, журнал состоит из последовательных записей, идущих подряд без промежутков между ними, иначе по
     * нему нельзя будет пройти от начала до конца без добавления существенного объема индексации в файле.
     * <p>
     * Итак, во-первых мы должны найти блок подходящего размера - он должен ТОЧНО совпадать или быть равным размеру
     * записи + HEADER_SIZE + 1. Итого имеем HEADER_SIZE + DATA + HEADER_SIZE + 1.
     * <p>
     * Технически, этот акт маркировки и повторного использования удаленных записей является формой сбора мусора.
     *
     * @param location     точка локации
     * @param recordLength длина нового блока для записи (HEADER_SIZE + длина записываемых данных)
     * @return обновленная точка локации
     */
    private JournalLocationData blockMarking(JournalLocationData location, int recordLength) {
        var records = emptyIdx.get(recordLength);
        if (records != null && !records.isEmpty()) {
            location.offset = records.pop();

            return location;
        }

        /*
        Невозможно изменить emptyIdx во время итерации,
        поэтому создаем список объектов для удаления (на самом деле они являются записями значения размера)
         */
        var toRemove = new IntArrayList();
        var newBlockSize = recordLength + Header.HEADER_SIZE + 1;

        // нет точного совпадения по размеру, ищем большой блок данных для записи
        for (var entry : emptyIdx.entrySet()) {
            var size = entry.getKey();
            var value = entry.getValue();

            /*
            Если мы хотим разделить эту запись, необходимо проверить, что есть достаточно места для новой записи и
            еще одной пустой записи с заголовком и хотя бы одним байтом для данных (newBlockSize).
             */
            if (size >= newBlockSize) {
                records = value;
                if (records == null || records.isEmpty()) {
                    /*
                    Это была последняя пустая запись такого размера, поэтому удаляем запись в индексе и
                    продолжите поиск более крупной пустой области (если есть)
                     */
                    toRemove.add(size);
                } else {
                    location.offset = records.pop();

                    // Нам нужно добавить пустую запись после новой записи с учетом размера заголовка
                    location.newEmptyRecordSize = size - recordLength - Header.HEADER_SIZE;

                    var newOffset = (int) location.offset + recordLength + Header.HEADER_SIZE;

                    storeEmptyRecord(newOffset, location.newEmptyRecordSize);
                    break;
                }
            }
        }

        toRemove.forEach(emptyIdx::remove);

        return location;
    }

    /**
     * Сохраняем новый пустой блок
     *
     * @param offset смещение нового блока
     * @param length размер нового блока
     */
    protected void storeEmptyRecord(long offset, Integer length) {
        var emptyRecs = emptyIdx.computeIfAbsent(length, key -> new LongArrayStack());

        // Добавляем указатель (смещение файла) к новой пустой записи
        emptyRecs.push(offset);
    }


    // для файлового хранилища

    /**
     * Перебираем содержимое журнала и
     * создаем индексы для пустых блоков
     *
     * @return позиция (смещение) для вставки новых блоков
     */
    protected long scanJournal() {
        clearEmptyIdx();

    }

    private void clearEmptyIdx() {
        for (var pointers : emptyIdx.values()) {
            pointers.clear();
        }
        emptyIdx.clear();

    }

    protected abstract HashBase createIndexJournal();

    protected abstract ByteBuffer createBuffer();

    protected static class JournalLocationData {
        long offset;
        int newEmptyRecordSize;
    }

    /**
     * Заголовок блока с данными
     * Boolean    - Индикатор активности записи
     * Byte       - Тип сообщения (0=Empty, 1=Bytes, 2=String и т.д.)
     * Integer    - Размер полезных данных в блоке (без учета заголовка)
     */
    protected static class Header implements Serializable {
        byte active;            // 1 byte
        byte type;              // 1 byte
        int size;               // 4 bytes
        static final int HEADER_SIZE = Integer.BYTES + 2;
    }
}
