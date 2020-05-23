package com.rurocker.demo.kafkasecurity.dto;

public class TestDto {

    private Long testId;
    private String testName;

    public TestDto() {
        this(null, null);
    }

    public TestDto(final Long testId, final String testName) {
        super();
        this.testId = testId;
        this.testName = testName;
    }

    public Long getTestId() {
        return this.testId;
    }

    public void setTestId(final Long testId) {
        this.testId = testId;
    }

    public String getTestName() {
        return this.testName;
    }

    public void setTestName(final String testName) {
        this.testName = testName;
    }

}
