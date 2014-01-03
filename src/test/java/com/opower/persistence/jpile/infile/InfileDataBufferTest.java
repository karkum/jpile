package com.opower.persistence.jpile.infile;

import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for the InfileDataBuffer
 *
 * @author aaron.silverman
 */
public class InfileDataBufferTest {
    private InfileDataBuffer infileDataBuffer;
    private static final String TIMESTAMP_STRING = "2000-01-10 08:00:01";
    private static final String DATE_STRING = "2000-01-10";
    private static final String TIME_STRING = "08:00:01";
    private static final Date TEST_DATE = new Date(100, 0 , 10, 8 , 0, 1);

    @Before
    public void setUp() {
        infileDataBuffer = new InfileDataBuffer();
    }

    @Test
    public void testAppendString() {
        String contents = "Gladiator is the best movie ever!";
        infileDataBuffer.append(contents);
        addRowAndAssertContents(contents);
    }

    @Test
    public void testAppendStringNeedingEscaping() {
        String contents = "C:\\windows\\bluescreen.png";
        infileDataBuffer.append(contents);
        addRowAndAssertContents(contents.replace("\\", "\\\\"));
    }

    @Test
    public void testAppendByte() {
        infileDataBuffer.append((byte) 65);
        addRowAndAssertContents("A");
    }

    @Test
    public void testAppendByteNeedingEscaping() {
        infileDataBuffer.append((byte) 92);
        addRowAndAssertContents("\\\\");
    }

    @Test
    public void testAppendBytes() {
        byte[] bytes = {72, 101, 108, 108, 111, 33};
        infileDataBuffer.append(bytes);
        addRowAndAssertContents("Hello!");
    }

    @Test
    public void testAppendBytesNeedingEscaping() {
        byte[] bytes = {67, 58, 92};
        infileDataBuffer.append(bytes);
        addRowAndAssertContents("C:\\\\");
    }

    @Test
    public void testAppendNull() throws Exception {
        infileDataBuffer.appendNull();
        addRowAndAssertContents("\\N");
    }

    @Test(expected = NullPointerException.class)
    public void testTemporalAnnotationTestClass() throws NoSuchMethodException {
        infileDataBuffer.append(TEST_DATE, TemporalAnnotationTestClass.class.getMethod("getDate", null));
    }

    @Test
    public void testTemporalDateAnnotation() throws NoSuchMethodException {
        infileDataBuffer.append(TEST_DATE, TemporalAnnotationTestClass.class.getMethod("getDateWithTemporal", null));
        addRowAndAssertContents(DATE_STRING);
    }

    @Test
    public void testTemporalTimeAnnotation() throws NoSuchMethodException {
        infileDataBuffer.append(TEST_DATE, TemporalAnnotationTestClass.class.getMethod("getTimeWithTemporal", null));
        addRowAndAssertContents(TIME_STRING);
    }

    @Test
    public void testTemporalTimestampAnnotation() throws NoSuchMethodException {
        infileDataBuffer.append(TEST_DATE, TemporalAnnotationTestClass.class.getMethod("getTimestampWithTemporal", null));
        addRowAndAssertContents(TIMESTAMP_STRING);
    }

    @Test
    public void testNullDateWithTemporal() throws NoSuchMethodException {
        infileDataBuffer.append(null, TemporalAnnotationTestClass.class.getMethod("getTimestampWithTemporal", null));
        addRowAndAssertContents("\\N");
    }

    private void addRowAndAssertContents(String expected) {
        try {
            infileDataBuffer.addRowToInfile();
            assertEquals(expected, CharStreams.toString(new InputStreamReader(infileDataBuffer.asInputStream())));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Entity
    @Table
    private class TemporalAnnotationTestClass {
        private Date date;
        private Date dateWithTemporal;
        private Date timeWithTemporal;
        private Date timestampWithTemporal;

        @Column(name = "date")
        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        @Temporal(TemporalType.DATE)
        @Column(name = "date_with_temporal")
        public Date getDateWithTemporal() {
            return dateWithTemporal;
        }

        public void setDateWithTemporal(Date dateWithTemporal) {
            this.dateWithTemporal = dateWithTemporal;
        }

        @Temporal(TemporalType.TIME)
        @Column(name = "time_with_temporal")
        public Date getTimeWithTemporal() {
            return timeWithTemporal;
        }

        public void setTimeWithTemporal(Date timeWithTemporal) {
            this.timeWithTemporal = timeWithTemporal;
        }

        @Temporal(TemporalType.TIMESTAMP)
        @Column(name = "timestamp_with_temporal")
        public Date getTimestampWithTemporal() {
            return timestampWithTemporal;
        }

        public void setTimestampWithTemporal(Date timestampWithTemporal) {
            this.timestampWithTemporal = timestampWithTemporal;
        }
    }
}
