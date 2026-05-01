package ru.bigdata.lab2.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class StackOverflowPost implements Serializable {

    private long id;
    private int postTypeId;
    private LocalDateTime creationDate;
    private int year;
    private String title;
    private String tags;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getPostTypeId() {
        return postTypeId;
    }

    public void setPostTypeId(int postTypeId) {
        this.postTypeId = postTypeId;
    }

    public LocalDateTime getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(LocalDateTime creationDate) {
        this.creationDate = creationDate;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }
}
