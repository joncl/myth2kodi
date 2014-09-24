myth2xbmc
=========

A script for generating a library of TV Show recordings for XBMC from a MythTV backend. Recordings are symlinked, and metadata and image links (posters, fanart, and banners) for each series are pulled from either TheTVDB or TheMovieDB depending on the "inetref" value in MythTV. "Specials" (episodes with the same series title, but missing show and episode info) are grouped together under the same series for easy navigation in XBMC.

Credits: some code original borrowed from another project **mythPLEX**: [thread](https://forums.plex.tv/index.php/topic/118748-connect-your-mythtv-recordings-to-plex/), [on GitHub](https://github.com/ascagnel/mythPlex).