window.refreshUiIcons = function (root = document) {
    if (!root) return;
    if (window.twemoji) {
        window.twemoji.parse(root, {
            folder: 'svg',
            ext: '.svg',
            base: 'https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/'
        });
    }
    window.lucide?.createIcons({ root, attrs: { 'stroke-width': 1.75 } });
};

window.refreshLucideIcons = window.refreshUiIcons;
