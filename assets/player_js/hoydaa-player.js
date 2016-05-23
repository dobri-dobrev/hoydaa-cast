var HLS_URL = 'https://storage.googleapis.com/hoydaa-tv-us-hls-master/cnn_turk/v2/gender_unknown/index.m3u8';
initializePlayer();
function initializePlayer(){
	var player = videojs('example-video');
	player.src([
	  { type: "application/x-mpegURL", src: HLS_URL }
		]);
	player.play();
}
function testPlayer(url){
	HLS_URL = url;
	initializePlayer();
}



