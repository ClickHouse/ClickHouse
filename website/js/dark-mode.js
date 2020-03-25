function darkMode() {
    var mode = localStorage.getItem('dark-mode');
    if (mode === '1') {
        document.getElementsByTagName('body')[0].className = 'dark-mode';
    } else if (mode === '-1') {
        document.getElementsByTagName('body')[0].className = 'light-mode';
    }
}
darkMode();
