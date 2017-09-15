package model;

import java.io.Serializable;

public class Rating implements Serializable{

    private static final long serialVersionUID = 1L;
    private Integer userId;
    private Integer movieId;
    private Double rating;

    public Rating(Integer userId, Integer movieId, Double rating) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }
}
