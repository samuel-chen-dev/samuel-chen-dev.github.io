<template>
  <div id="app">
    <router-view />
  </div>
</template>

<script setup>
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body {
  font-family: 'Microsoft YaHei', 'PingFang SC', sans-serif;
  background: #1a0a2e;
  color: #e0d8c8;
  min-height: 100vh;
}

#app {
  min-height: 100vh;
}
</style>