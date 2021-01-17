package HudiSpark;

import java.util.List;

public class Album {

    private int albumId;
    private String title;
    private List<String> tracks;
    private Long updateDate;

    public Album(int albumId, String title, List<String> tracks, Long updateDate) {
        this.albumId = albumId;
        this.title = title;
        this.tracks = tracks;
        this.updateDate = updateDate;
    }

    public int getAlbumId() {
        return albumId;
    }

    public void setAlbumId(int albumId) {
        this.albumId = albumId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getTracks() {
        return tracks;
    }

    public void setTracks(List<String> tracks) {
        this.tracks = tracks;
    }

    public Long getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(Long updateDate) {
        this.updateDate = updateDate;
    }
}
